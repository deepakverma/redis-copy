﻿using System;
using System.Collections.Concurrent;
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
        private ConnectionMultiplexer sourcecon;
        private ConnectionMultiplexer destcon;
        private string sourceEndpoint;
        private string destEndpoint;
        private bool flushdest = false;
        private bool flushdbconfirm = false;
        private int dbToCopy;
        private ConcurrentDictionary<string, double> TasksInProgress = new ConcurrentDictionary<string, double>();
        private double percent;
        private object progressLock = new object();
        //variables to track the console cursor        
        private int cursorTop; // top when the progress started
        private int totalcursorsinProgress = 0; // no of rows after which to print the results
        //end variable to track the cursor

        public bool allCopied = false;

        public RedisCopy(Options options)
        {

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
            destcon = GetConnectionMultiplexer(options.DestinationEndpoint, options.DestinationPort, configdestination);

            sourceEndpoint = options.SourceEndpoint;
            destEndpoint = options.DestinationEndpoint;
            flushdest = options.DestinationFlush;
            dbToCopy = options.DBToCopy;
            //overwritedest = options.OverwriteDestination;
        }

        public void Copy()
        {
            Console.CursorVisible = false;
            CopyConcurrent();
            while (!this.allCopied)  //block the main thread
            {
                Thread.Sleep(1000);
                var temp = Console.CursorTop;
                PrintTasksInProgress();
                Console.CursorTop = temp;
            }
            Console.CursorVisible = true;
            Console.CursorLeft = 0;
            Console.WriteLine();
            Console.WriteLine("Done copying");
            Console.WriteLine("!!Important");
            Console.WriteLine("!!This tool does a point in time copy of data:expired keys or new data being written to source during transfer was not copied");
            Console.WriteLine("Please check the source and destination keys to make sure the copy was successful");
            Console.WriteLine();
        }

        private void PrintTasksInProgress()
        {
            var temp = cursorTop;

            foreach (var p in TasksInProgress)
            {
                Console.CursorTop = temp++;
                Console.Write($"\r{p.Key.Replace("Unspecified/", "")} => {destEndpoint} ({Math.Round(p.Value)}%)");
            }

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
            } catch
            {
                return null;
            }
        }

        private async Task<Tuple<long, double>> Copy(ConnectionMultiplexer connToSource)
        {
            long totalKeysSource = GetTotalKeysFromInfo(connToSource);
            long totalKeysCopied = 0;
            var sourcedb = connToSource.GetDatabase(dbToCopy);
            var destdb = destcon.GetDatabase(dbToCopy);
            double percent;
            var source = connToSource.GetEndPoints()[0].ToString();
            TasksInProgress[source] = 0.0;
            Console.WriteLine("***Performing a point in time copy of data:expired keys or new data being written to source during transfer won't be copied***");
            Stopwatch sw = Stopwatch.StartNew();
            IEnumerable<RedisKey> scanResults;
            long totalCountToBeCopied = 0;
            do
            {
                scanResults = connToSource.GetServer(connToSource.GetEndPoints()[0]).Keys(dbToCopy); //SE.Redis internally calls SCAN here
                Interlocked.Add(ref totalCountToBeCopied, scanResults.Count());

                foreach (var key in scanResults) 
                {
                    sourcedb.KeyTimeToLiveAsync(key).ContinueWith(async ttl =>
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
                                        if (restore.IsFaulted || restore.IsCanceled)
                                        {
                                            throw new AggregateException(restore.Exception);
                                        }
                                        else
                                        {
                                            Interlocked.Increment(ref totalKeysCopied);
                                            Interlocked.Decrement(ref totalCountToBeCopied);
                                            percent = ((double)totalKeysCopied / totalKeysSource) * 100;
                                            TasksInProgress[source] = percent;
                                        }
                                    });
                                }
                            }
                            );
                        }
                    });
                }

            } while (scanResults.Count() > 0);
            //block to monitor completion
            while (Interlocked.Read(ref totalCountToBeCopied) > 0)
            {
                await Task.Delay(500);
            }
            sw.Stop();
            TasksInProgress[source] = 100;
            return new Tuple<long, double>(totalKeysCopied, Math.Round(sw.Elapsed.TotalSeconds));
        }

        private long GetTotalKeysFromInfo(ConnectionMultiplexer conn)
        {
            var keyspace = conn.GetServer(conn.GetEndPoints()[0]).Info("keyspace");
            long result = 0;
            var k = keyspace.FirstOrDefault();
            if (k != null)
            {
                var v = k.ElementAtOrDefault(dbToCopy);
                if (!v.Equals(new KeyValuePair<string, string>()))
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
                    List<Task> flushtasks = new List<Task>();
                    if (flushdbconfirm)
                    {
                        foreach (var node in destClusternodes.Nodes)
                        {
                            if (!node.IsSlave)
                            {
                                flushtasks.Add(Task.Run(() =>
                                {
                                    Console.WriteLine($"Flushing db {dbToCopy} on destination {destEndpoint}:{(node.EndPoint as IPEndPoint).Port}");
                                    var conn = GetConnectionMultiplexer(destEndpoint, (node.EndPoint as IPEndPoint).Port, config);
                                    conn.GetServer(conn.GetEndPoints()[0]).FlushDatabaseAsync(dbToCopy);
                                    conn.Close();
                                }));
                            }
                        }
                        Task.WaitAll(flushtasks.ToArray());
                    }
                } else
                {
                    if (flushdbconfirm)
                    {
                        Console.WriteLine($"Flushing db {dbToCopy} on destination {destination}");
                        destcon.GetServer(destcon.GetEndPoints()[0]).FlushDatabaseAsync(dbToCopy);
                    }
                }
            }
        }

        private async Task CopyConcurrent()
        {
            FlushdbIfOpted();
            cursorTop = Console.CursorTop;
            //if it's clustered cache loop on each of the shard and copy
            var sourceClusternodes = GetClusterNodes(sourcecon);
            if (sourceClusternodes != null)
            {
                var config = ConfigurationOptions.Parse(sourcecon.Configuration);
                List<Tuple<string, long, double>> results = new List<Tuple<string, long, double>>();
                List<Task> copytasks = new List<Task>();
                foreach (var node in sourceClusternodes.Nodes)
                {
                    if (!node.IsSlave)
                    {
                        //Console.WriteLine($"Copying keys from {sourceEndpointstring}");
                        copytasks.Add(Task.Run(async () =>
                        {
                            var shardcon = GetConnectionMultiplexer(sourceEndpoint, (node.EndPoint as IPEndPoint).Port, config);
                            var r = await Copy(shardcon);
                            var sourceEndpointstring = $"{sourceEndpoint}:{(node.EndPoint as IPEndPoint).Port}";
                            results.Add(new Tuple<string, long, double>(sourceEndpointstring, r.Item1, r.Item2));
                            shardcon.Close();
                        }));
                        totalcursorsinProgress++;
                    }
                }
                await Task.WhenAll(copytasks);
                var cursor = cursorTop + totalcursorsinProgress;
                foreach (var result in results)
                {
                    Console.CursorTop = cursor++;
                    Console.CursorLeft = 0;
                    Console.WriteLine($"Copied {result.Item2} keys from {result.Item1} to {destEndpoint} in {result.Item3} seconds\n");
                }
                allCopied = true;
            } else
            {
                //non clustered cache
                //Console.WriteLine($"Copying keys from {sourceEndpoint}");
                var result = await Copy(sourcecon);
                totalcursorsinProgress++;
                Console.CursorTop = cursorTop + totalcursorsinProgress;
                Console.WriteLine($"Copied {result.Item1} keys from {sourceEndpoint} to {destEndpoint} in {result.Item2} seconds");
                allCopied = true;
            }
            sourcecon.Close();
            destcon.Close();
        }
    }
}
