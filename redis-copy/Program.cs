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
                copy.Copy();
            }
        }
    }
}

