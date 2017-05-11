using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis;
using CommandLine;
namespace redis_copy
{
    class Program
    {
        

        static void Main(string[] args)
        {
            var options = Options.Parse(args);
            if (options != null)
            {
                var copy = new RedisCopy(options);
                Progress<long> progress = new Progress<long>();
                progress.ProgressChanged += Progress_ProgressChanged;
                copy.Copy(progress);
                Console.WriteLine($"\n{copy.TotalKeysCopiedToDestination} keys copied in {copy.TotalTimeTakenToCopySeconds} seconds");
            }
        }

        private static void Progress_ProgressChanged(Object sender, long TotalKeysCopiedToDestination)
        {
            Console.Write($"Keys copied {TotalKeysCopiedToDestination}\r");
        }
    }
}

