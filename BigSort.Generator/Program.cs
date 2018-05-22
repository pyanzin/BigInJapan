using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BigSort.Generator {
    class Program {

        static int MaxStringLength = 1024;

        private static FileStream _output;

        private static long FileSize;
        
        static void Main(string[] args)
        {
            int fileSizeMb = 8096;
            if (args.Length > 0)
                fileSizeMb = int.Parse(args[0]);
           
            FileSize = 1024L * 1024 * fileSizeMb;
            
            var begin = DateTime.UtcNow;
            var fileName = "unsorted.txt";

            _output = File.Create(fileName, 1024 * 1024 * 256);

            ThreadPool.QueueUserWorkItem(GenerateBlockWrapper);
            ThreadPool.QueueUserWorkItem(GenerateBlockWrapper);
            ThreadPool.QueueUserWorkItem(GenerateBlockWrapper);
            ThreadPool.QueueUserWorkItem(GenerateBlockWrapper);

            Console.ReadKey();
        }


        static void GenerateBlockWrapper(object context)
        {
            GenerateBlock(1024 * 1024 * 128, FileSize / 4);
        }

        static void GenerateBlock(long blockSize, long taskLimit)
        {
            long totallyGenerated = 0;
            var block = new byte[blockSize + 1024 + 64];
            int blockIndex = 0;
            Random rnd = new Random();

            for (;;)
            {
                String numberString = rnd.Next().ToString();

                var numberBytes = Encoding.ASCII.GetBytes(numberString);
                Array.Copy(numberBytes, 0, block, blockIndex, numberBytes.Length);

                blockIndex += numberString.Length;
                totallyGenerated += numberString.Length;

                block[++blockIndex] = (byte) '.';
                block[++blockIndex] = (byte) ' ';
                ++blockIndex;

                var stringPartSize = rnd.Next(1, MaxStringLength);

                byte[] stringPartArray = new byte[stringPartSize];
                rnd.NextBytes(stringPartArray);
                for (int i = 0; i < stringPartArray.Length; i++)
                    stringPartArray[i] = (byte) (stringPartArray[i] % 52 + (byte) 'A');

                Array.Copy(stringPartArray, 0, block, blockIndex, stringPartArray.Length);

                blockIndex += stringPartSize;
                block[++blockIndex] = (byte) '\n';
                block[++blockIndex] = (byte) '\r';
                ++blockIndex;

                totallyGenerated += stringPartSize + 2;

                if (totallyGenerated >= taskLimit)
                {
                    WriteBlock(block, blockIndex);
                    return;
                }

                if (blockIndex >= blockSize)
                {
                    WriteBlock(block, blockIndex);
                    blockIndex = 0;
                }
            }
        }

        private static object _outputFileLock = new object();
        
        static void WriteBlock(byte[] block, int count)
        {
            lock (_outputFileLock)
            {
                _output.Write(block, 0, count);
            }
        }
        
    }
}
