using System;

namespace EHFileSender
{
    public static class ConsoleLog
    {
        public enum LogLevel
        {
            Default,
            Ok,
            Warning,
            Error
        }

        public static void WriteLog(string logText)
        {
            WriteLog(LogLevel.Default, logText);
        }

        public static void WriteLog(LogLevel level, string logText)
        {
            switch (level)
            {
                case LogLevel.Ok:
                    Console.ForegroundColor = ConsoleColor.Green;
                    break;
                case LogLevel.Warning:
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    break;
                case LogLevel.Error:
                    Console.ForegroundColor = ConsoleColor.Red;
                    break;
            }
            Console.WriteLine($"{DateTime.Now:HH:mm:ss.ffff}: {logText}");
            Console.ResetColor();
        }
    }
}
