using System;
using System.Collections.Generic;
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

        [Option("sp", Required = false, Default = 6380, HelpText = "Source port" )]
        public int SourcePort { get; set; }

        [Option("sssl", Required = false, Default = true, HelpText = "Connect Source over ssl" )]
        public bool SourceSSL { get; set; }

        [Option("db", Required = false, Default = 0, HelpText = "DB to Copy")]
        public int DBToCopy { get; set; }

        [Option("de", Required = true, HelpText = "DestinationEndpoint *.redis.cache.windows.net") ]
        public string DestinationEndpoint { get; set; }

        [Option("da", Required = true, HelpText = "Destination Password") ]
        public string DestinationPassword { get; set; }

        [Option("dp", Required = false, Default = 6380, HelpText = "Destination port" )]
        public int DestinationPort { get; set; }

        [Option("dssl", Required = false, Default = true, HelpText = "Destination Source over ssl" )]
        public bool DestinationSSL { get; set; }

        [Option("flushdest", Required = false, Default = false, HelpText = "Flush destination cache before copying")]
        public bool DestinationFlush { get; set; }

        //[Option("overwritedest", Required = false, Default = false, HelpText = "Overwrites existing key in destination")]
        //public bool OverwriteDestination { get; set; }

        public static Options Parse(string[] args)
        {
            Options options = null;
            var parserResult = CommandLine.Parser.Default.ParseArguments<Options>(args);

            parserResult.WithParsed<Options>(opts => options = opts)
            .WithNotParsed<Options>((errs) =>
            {
                var helpText = HelpText.AutoBuild(parserResult, h =>
                {
                return HelpText.DefaultParsingErrorsHandler(parserResult, h); 
                }, e =>
                {
                    return e;
                });
            });
            return options;
        }
    }
}