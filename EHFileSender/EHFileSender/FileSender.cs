using Microsoft.Azure.EventHubs;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using static EHFileSender.ConsoleLog;

namespace EHFileSender
{
    public class FileSender
    {
        private readonly bool _detailedReporting;
        private readonly bool _testForJson;
        private readonly bool _giveUpAfterNonJson;
        private readonly int _degreeOfParallelism;
        private readonly bool _simulateSending;
        private readonly string _eventHubConnectionString;
        private readonly string _fileName;
        private const int MAX_EVENT_SIZE = 262144;
        private const int EVENT_PADDING_BYTES = 16;

        public FileSender(string eventHubConnectionString, string fileName, bool detailedReporting, bool testForJson, bool giveUpAfterNonJson, int degreeOfParallelism, bool simulateSending)
        {
            _fileName = fileName;
            _detailedReporting = detailedReporting;
            _giveUpAfterNonJson = giveUpAfterNonJson;
            _degreeOfParallelism = degreeOfParallelism;
            _eventHubConnectionString = eventHubConnectionString;
            _simulateSending = simulateSending;
        }

        public long ProcessFile()
        {
            WriteLog($"Spinning up {_degreeOfParallelism} Event Hub batch senders");
            EventBatch[] senders = new EventBatch[_degreeOfParallelism];
            for (int clientCount = 0; clientCount < _degreeOfParallelism; clientCount++)
            {
                senders[clientCount] = new EventBatch(EventHubClient.CreateFromConnectionString(_eventHubConnectionString), _simulateSending);
            }

            List<Task> tasks = new List<Task>(_degreeOfParallelism);
            TaskFactory tf = new TaskFactory(TaskCreationOptions.LongRunning, TaskContinuationOptions.LongRunning);

            WriteLog($"Opened file {_fileName}");

            long i = 0;

            List<EventData> eventData = new List<EventData>();
            int runningEventSize = 0;

            foreach (string line in File.ReadLines(_fileName))
            {
                if (_testForJson)
                {
                    try
                    {
                        dynamic obj = JsonConvert.DeserializeObject(line);
                    }
                    catch (JsonException je)
                    {
                        if (_giveUpAfterNonJson)
                        {
                            throw new Exception($"Found non-JSON garbage in file {_fileName} on line {i}, giving up on reading it. Garbage line was {line}. JsonException was {je.Message}.");
                        }
                        else
                        {
                            WriteLog(LogLevel.Warning, $"  Found non-JSON garbage in file {_fileName} on line {i}, moving onto next line. JsonException was {je.Message}.");
                            continue; //skip this line
                        }
                    }
                }

                Interlocked.Increment(ref i);
                byte[] eventBytes = new UTF8Encoding().GetBytes(line);
                int currEventSize = eventBytes.Length + EVENT_PADDING_BYTES;

                if (runningEventSize + currEventSize > MAX_EVENT_SIZE)
                {
                    EventBatch batch = senders.First(s => s.Busy == false);
                    tasks.Add(tf.StartNew(async () => await batch.SendPacket(eventData.ToArray())));

                    eventData.Clear();
                    runningEventSize = 0;

                    WriteLog($"  Sent {i} events");

                    if (tasks.Count() >= _degreeOfParallelism)
                    {
                        if (_detailedReporting)
                        {
                            WriteLog($"     Dispatching {_degreeOfParallelism} tasks.");

                            foreach (TaskStatus taskStatus in Enum.GetValues(typeof(TaskStatus)))
                            {
                                WriteLog($"     {tasks.Count(x => x.Status == taskStatus)}/{tasks.Count()} tasks of status {taskStatus.ToString()}.");
                            }
                        }
                        Task.WaitAll(tasks.ToArray());

                        tasks.Clear();
                    }
                }
                eventData.Add(new EventData(eventBytes));

                runningEventSize += currEventSize;
            }


            if (tasks.Count() > 0)
            {
                if (_detailedReporting)
                {
                    WriteLog($"     Dispatching final batch of {tasks.Count()} tasks.");
                    foreach (TaskStatus taskStatus in Enum.GetValues(typeof(TaskStatus)))
                    {
                        WriteLog($"     {tasks.Count(x => x.Status == taskStatus)}/{tasks.Count()} tasks of status {taskStatus.ToString()}.");
                    }
                }
                tf.ContinueWhenAll(tasks.ToArray(), (t) => WriteLog("  Done"));
            }

            return i;
        }
    }
}
