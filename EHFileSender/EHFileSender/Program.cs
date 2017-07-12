using System;
using System.IO;
using System.Linq;
using Microsoft.Azure.EventHubs;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Collections.Generic;
using static EHFileSender.ConsoleLog;

namespace EHFileSender
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Count() == 0)
            {
                Console.WriteLine("Please specify the path to a file.");
                Console.ReadKey();
                return;
            }

            string fileName = args[0];
            if (!File.Exists(fileName))
            {
                WriteLog(LogLevel.Error, $"File {fileName} doesn't exist. Aborting.");
                Console.ReadKey();
                return;
            }

            var cb = new ConfigurationBuilder()
                .AddJsonFile("./appsettings.json");

            var config = cb.Build();

            string eventHubConnectionString = config["eventHubConnStr"];
            bool detailedReporting = bool.Parse(config["detailedReporting"]);
            bool simulateSending = bool.Parse(config["simulateSending"]);
            bool testForJson = bool.Parse(config["testForJson"]);
            bool giveUpAfterNonJson = bool.Parse(config["giveUpAfterNonJson"]);
            int degreeOfParallelism = int.Parse(config["degreeOfParallelism"]);

            FileSender sender = new FileSender(eventHubConnectionString, fileName, detailedReporting, testForJson, giveUpAfterNonJson, degreeOfParallelism, simulateSending);

            try
            {
                long linesProcessed = sender.ProcessFile();
                WriteLog(LogLevel.Ok, $"Completed file {fileName} after {linesProcessed} lines.");
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine();
            }
            catch (Exception e)
            {
                WriteLog(LogLevel.Error, $"Caught exception {e.Message}");
            }

            Console.ReadKey();
        }




    }
}