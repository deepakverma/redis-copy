using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
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
            configsource.EndPoints.Add(options.SourceEndpoint, options.SourcePort);
            configsource.Ssl = options.SourceSSL;
            configsource.Password = options.SourcePassword;
            configsource.AllowAdmin = true;
            configsource.SyncTimeout = 60000; // increasing timeout for source for SCAN command
            sourcecon = ConnectionMultiplexer.Connect(configsource);

            ConfigurationOptions configdestination = new ConfigurationOptions();
            configdestination.EndPoints.Add(options.DestinationEndpoint, options.DestinationPort);
            configdestination.Ssl = options.DestinationSSL;
            configdestination.Password = options.DestinationPassword;
            configdestination.AllowAdmin = true;
            destcon = ConnectionMultiplexer.Connect(configdestination);

        }

        public void Copy(int dbToCopy, IProgress<long> progress)
        {
            totalKeysCopied = 0;
            IsCopyComplete = false;
            InfoTotalKeysSource = GetTotalKeysFromInfo(dbToCopy, sourcecon);

            var sourcedb = sourcecon.GetDatabase(dbToCopy);
            var destdb = destcon.GetDatabase(dbToCopy);
            
            sw = Stopwatch.StartNew();
            foreach (var key in sourcecon.GetServer(sourcecon.GetEndPoints()[0]).Keys(dbToCopy)) //SE.Redis internally calls SCAN here
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
            TotalTimeTakenToCopySeconds = sw.Elapsed.TotalSeconds;
            InfoTotalKeysDestinationAfterCompletion = GetTotalKeysFromInfo(dbToCopy, destcon);
            if (InfoTotalKeysDestinationAfterCompletion < InfoTotalKeysSource)
            {
                throw new Exception($"source key count={InfoTotalKeysSource} doesn't match destination key count={InfoTotalKeysDestinationAfterCompletion}");
            }
        }
        
        private long GetTotalKeysFromInfo(int dbToCopy, ConnectionMultiplexer conn)
        {
            var keyspace = conn.GetServer(conn.GetEndPoints()[0]).Info("keyspace");
            return long.Parse(keyspace.First().ElementAt(dbToCopy).Value.Split(new char[] { ',' })[0].Split(new char[] { '=' })[1]);
        }

    }
}
