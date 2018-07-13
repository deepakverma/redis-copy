using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace redis_copy
{
    class RedisCopy
    {
        private long totalKeysCopied;
        private Stopwatch sw;
        private ConnectionMultiplexer sourcecon;
        private ConnectionMultiplexer destcon;
        private string sourceEndpoint;
        private string destEndpoint;
        private bool flushdest = false;
        private bool flushdbconfirm = false;
        private int dbToCopy;

        public Progress<long> Progress { get; private set; } 
        public double TotalTimeTakenToCopySeconds { get; private set; }
        public long InfoTotalKeysSource { get; private set; }
        public long InfoTotalKeysDestinationAfterCompletion { get; private set; }
        public long TotalKeysCopiedToDestination => totalKeysCopied;
        

        public bool IsCopyComplete { get; private set; }

        public RedisCopy(Options options)
        {
            Progress = new Progress<long>();

            ConfigurationOptions configsource = new ConfigurationOptions();
            configsource.Ssl = options.SourceSSL;
            configsource.Password = options.SourcePassword;
            configsource.AllowAdmin = true;
            configsource.SyncTimeout = 60000; // increasing timeout for source for SCAN command
            sourcecon = GetConnectionMultiplexer(options.SourceEndpoint, options.SourcePort, configsource);
            
            ConfigurationOptions configdestination = new ConfigurationOptions();
            configdestination.Ssl = options.DestinationSSL;
            configdestination.Password = options.DestinationPassword;
            configdestination.AllowAdmin = true;
            destcon = GetConnectionMultiplexer(options.DestinationEndpoint,options.DestinationPort, configdestination);

            sourceEndpoint = options.SourceEndpoint;
            destEndpoint = options.DestinationEndpoint;
            flushdest = options.DestinationFlush;
            dbToCopy = options.DBToCopy;
            //overwritedest = options.OverwriteDestination;
        }

        private ConnectionMultiplexer GetConnectionMultiplexer(string host, int port, ConfigurationOptions config)
        {
            ConfigurationOptions tempconfig = new ConfigurationOptions();
            tempconfig.Ssl = config.Ssl;
            tempconfig.Password = config.Password;
            tempconfig.AllowAdmin = config.AllowAdmin;
            tempconfig.SyncTimeout = config.SyncTimeout;
            tempconfig.EndPoints.Add(host, port);
            return ConnectionMultiplexer.Connect(tempconfig);
        }

        private ClusterConfiguration GetClusterNodes(ConnectionMultiplexer conn)
        {
            try
            {
                return conn.GetServer(conn.GetEndPoints()[0]).ClusterNodes();
            }
            catch
            {
                return null;
            }
        }

        private void Copy(IProgress<long> progress, ConnectionMultiplexer connToSource)
        {
            totalKeysCopied = 0;
            IsCopyComplete = false;
            InfoTotalKeysSource = GetTotalKeysFromInfo(connToSource);
            if(InfoTotalKeysSource == 0)
            {
                Console.WriteLine($"no keys found in db {dbToCopy}");
                return;
            }
            TotalTimeTakenToCopySeconds = 0;
            var sourcedb = connToSource.GetDatabase(dbToCopy);
            var destdb = destcon.GetDatabase(dbToCopy);
            sw = Stopwatch.StartNew();
            foreach (var key in connToSource.GetServer(connToSource.GetEndPoints()[0]).Keys(dbToCopy)) //SE.Redis internally calls SCAN here
            {
                sourcedb.KeyTimeToLiveAsync(key).ContinueWith(ttl =>
                {
                    if (ttl.IsFaulted || ttl.IsCanceled)
                    {
                        throw new AggregateException(ttl.Exception);
                    }
                    else
                    {
                        sourcedb.KeyDumpAsync(key).ContinueWith(dump =>
                        {
                            if (dump.IsFaulted || dump.IsCanceled)
                            {
                                throw new AggregateException(dump.Exception);
                            }
                            else
                            {
                               //Redis > 3.0, if key already exists it won't overwrite
                               destdb.KeyRestoreAsync(key, dump.Result, ttl.Result).ContinueWith(restore =>
                               {
                                   Interlocked.Increment(ref totalKeysCopied);
                                   if (totalKeysCopied % 10 == 0)
                                   {
                                       progress.Report(totalKeysCopied);
                                   }
                               });
                            }
                        }
                        );
                    }
                });
            }
            
            //block to monitor completion
            while (TotalKeysCopiedToDestination < InfoTotalKeysSource);
            sw.Stop();
            TotalTimeTakenToCopySeconds = Math.Round(sw.Elapsed.TotalSeconds);
            //InfoTotalKeysDestinationAfterCompletion = GetTotalKeysFromInfo(dbToCopy, destcon);
            //if (InfoTotalKeysDestinationAfterCompletion < InfoTotalKeysSource)
            //{
            //    Console.WriteLine($"!Warning: source key count={InfoTotalKeysSource} doesn't match destination key count={InfoTotalKeysDestinationAfterCompletion}.");
            //}
        }
        
        private long GetTotalKeysFromInfo(ConnectionMultiplexer conn)
        {
            var keyspace = conn.GetServer(conn.GetEndPoints()[0]).Info("keyspace");
            long result = 0;
            var k = keyspace.FirstOrDefault();
            if (k != null)
            {
                var v = k.ElementAtOrDefault(dbToCopy);
                if (!v.Equals(new KeyValuePair<string,string>()))
                {
                    long.TryParse(v.Value.Split(new char[] { ',' })[0].Split(new char[] { '=' })[1], out result);
                }
            }
            return result;
        }

        private void FlushdbIfOpted()
        {
            if (flushdest)
            {
                var destination = destcon.GetEndPoints()[0].ToString().Replace("Unspecified/", "");
                if (!flushdbconfirm)
                {
                    Console.Write($"Are you sure you want to flush db {dbToCopy} of {destEndpoint} before copying (y/n)?");
                    char answ = Console.ReadKey().KeyChar;
                    Console.WriteLine();
                    if (answ != 'y' && answ != 'Y')
                    {
                        Console.WriteLine("Aborting");
                        Environment.Exit(1);
                        return; //this is not required
                    } else
                    {
                        flushdbconfirm = true;
                    }
                }
                var destClusternodes = GetClusterNodes(destcon);
                if (destClusternodes != null)
                {
                    var config = ConfigurationOptions.Parse(destcon.Configuration);

                    foreach (var node in destClusternodes.Nodes)
                    {
                        if (!node.IsSlave)
                        {
                            if (flushdbconfirm)
                            {
                                Console.WriteLine($"Flushing db {dbToCopy} on destination {destEndpoint}:{(node.EndPoint as IPEndPoint).Port}");
                                var conn = GetConnectionMultiplexer(destEndpoint, (node.EndPoint as IPEndPoint).Port, config);
                                conn.GetServer(conn.GetEndPoints()[0]).FlushDatabase(dbToCopy);
                                conn.Close();
                            }
                        }
                    }
                } else
                {
                    Console.WriteLine($"Flushing db {dbToCopy} on destination {destination}");
                    destcon.GetServer(destcon.GetEndPoints()[0]).FlushDatabase(dbToCopy);
                }
            }
        }

        public void Copy(int dbToCopy, IProgress<long> progress)
        {
            FlushdbIfOpted();
            //if it's clustered cache loop on each of the shard and copy
            var sourceClusternodes = GetClusterNodes(sourcecon);
            if (sourceClusternodes != null)
            {
                var config = ConfigurationOptions.Parse(sourcecon.Configuration);

                foreach (var node in sourceClusternodes.Nodes)
                {
                    if (!node.IsSlave)
                    {
                        var sourceEndpointstring = $"{sourceEndpoint}:{(node.EndPoint as IPEndPoint).Port}";
                        Console.WriteLine($"Copying keys from {sourceEndpointstring}");
                        Copy(progress, GetConnectionMultiplexer(sourceEndpoint, (node.EndPoint as IPEndPoint).Port, config));
                        Console.WriteLine($"\rCopied {TotalKeysCopiedToDestination} keys from {sourceEndpointstring} to {destEndpoint} in {TotalTimeTakenToCopySeconds} seconds\n");
                    }
                }
            } else
            {
                //non clustered cache
                Copy(progress, sourcecon);
                Console.WriteLine($"\rCopied {TotalKeysCopiedToDestination} keys from {sourceEndpoint} to {destEndpoint} in {TotalTimeTakenToCopySeconds} seconds");
            }
        }

    }
}
