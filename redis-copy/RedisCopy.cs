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
        private bool? flushdbconfirm = null;

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
            //overwritedest = options.OverwriteDestination;
        }

        private ConnectionMultiplexer GetConnectionMultiplexer(string host, int port, ConfigurationOptions config)
        {
            ConfigurationOptions shardconfig = new ConfigurationOptions();
            shardconfig.EndPoints.Add(host, port);
            shardconfig.Ssl = config.Ssl;
            shardconfig.Password = config.Password;
            shardconfig.AllowAdmin = config.AllowAdmin;
            shardconfig.SyncTimeout = config.SyncTimeout;
            return ConnectionMultiplexer.Connect(shardconfig);
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

        private void Copy(int dbToCopy, IProgress<long> progress, ConnectionMultiplexer conn)
        {
            totalKeysCopied = 0;
            IsCopyComplete = false;
            InfoTotalKeysSource = GetTotalKeysFromInfo(dbToCopy, conn);
            TotalTimeTakenToCopySeconds = 0;
            var sourcedb = conn.GetDatabase(dbToCopy);
            var destdb = destcon.GetDatabase(dbToCopy);
            sw = Stopwatch.StartNew();
            foreach (var key in conn.GetServer(conn.GetEndPoints()[0]).Keys(dbToCopy)) //SE.Redis internally calls SCAN here
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
        
        private long GetTotalKeysFromInfo(int dbToCopy, ConnectionMultiplexer conn)
        {
            var keyspace = conn.GetServer(conn.GetEndPoints()[0]).Info("keyspace");
            return long.Parse(keyspace.First().ElementAt(dbToCopy).Value.Split(new char[] { ',' })[0].Split(new char[] { '=' })[1]);
        }

        private void FlushdbsIfOpted()
        {
            if (flushdest)
            {
                var destination = destcon.GetEndPoints()[0].ToString().Replace("Unspecified/", "");
                if (!flushdbconfirm.HasValue)
                {
                    Console.Write($"Are you sure you want to flushalldatabases of {destEndpoint} before copying (y/n)?");
                    char answ = Console.ReadKey().KeyChar;
                    Console.WriteLine();
                    if (answ != 'y' && answ != 'Y')
                    {
                        Console.WriteLine("Aborting");
                        Environment.Exit(1);
                        return;
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
                            if (flushdbconfirm.HasValue && flushdbconfirm.Value)
                            {
                                Console.WriteLine($"Flushing destination {destEndpoint}:{(node.EndPoint as IPEndPoint).Port}");
                                var conn = GetConnectionMultiplexer(destEndpoint, (node.EndPoint as IPEndPoint).Port, config);
                                conn.GetServer(conn.GetEndPoints()[0]).FlushAllDatabases();
                                conn.Close();
                            }
                        }
                    }
                } else
                {
                    Console.WriteLine($"Flushing destination {destination}");
                    destcon.GetServer(destcon.GetEndPoints()[0]).FlushAllDatabases();
                }
            }
        }

        public void Copy(int dbToCopy, IProgress<long> progress)
        {
            FlushdbsIfOpted();
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
                        Copy(dbToCopy, progress, GetConnectionMultiplexer(sourceEndpoint, (node.EndPoint as IPEndPoint).Port, config));
                        Console.WriteLine($"\nCopied {TotalKeysCopiedToDestination} keys from {sourceEndpointstring} to {destEndpoint} in {TotalTimeTakenToCopySeconds} seconds\n");
                    }
                }
            } else
            {
                //non clustered cache
                Copy(dbToCopy, progress, sourcecon);
                Console.WriteLine($"\nCopied {TotalKeysCopiedToDestination} keys from {sourceEndpoint} to {destEndpoint} in {TotalTimeTakenToCopySeconds} seconds");
            }
        }

    }
}
