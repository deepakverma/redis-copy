using System;
using CommandLine;
using CommandLine.Text;

namespace redis_copy
{
    internal class Options
    {
        [Option("se", Required = true, HelpText = "SourceEndpoint *.redis.cache.windows.net") ]
        public string SourceEndpoint { get; set; }

        [Option("sa", Required = true, HelpText = "Source password") ]
        public string SourcePassword { get; set; }

        [Option("sp", Required = false, DefaultValue = 6380, HelpText = "Source port" )]
        public int SourcePort { get; set; }

        [Option("sssl", Required = false, DefaultValue = true, HelpText = "Connect Source over ssl" )]
        public bool sourceSSL { get; set; }

        [Option("de", Required = true, HelpText = "DestinationEndpoint *.redis.cache.windows.net") ]
        public string DestinationEndpoint { get; set; }

        [Option("da", Required = true, HelpText = "Destination Password") ]
        public string DestinationPassword { get; set; }

        [Option("dp", Required = false, DefaultValue = 6380, HelpText = "Destination port" )]
        public int DestinationPort { get; set; }

        [Option("dssl", Required = false, DefaultValue = true, HelpText = "Destination Source over ssl" )]
        public bool destinationSSL { get; set; }


        [HelpOption]
        public string GetUsage()
        {
            return HelpText.AutoBuild(this, text =>
            {
                HelpText.DefaultParsingErrorsHandler(this, text);
                Console.WriteLine("Usage:");
                Console.WriteLine($"redis-copy --se <sourceendpoint:port> --sa <sourcepassword> --de <endpoint:port> --da <destinationpassword>");
            });
        }

        public static Options Parse(string[] args)
        {
            Options options = new Options();
            if (!Parser.Default.ParseArguments(args, options))
            {
                return null;
            }
            return options;
        }

    }
}