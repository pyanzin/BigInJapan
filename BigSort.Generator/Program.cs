using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BigSort.Generator {
    class Program {
        private const int BLOCK_SIZE = 1024 * 1024 * 256;

        private static int _maxStringLength = 1024;

        private static FileStream _output;

        private static long _fileSize = 1024L * 1024 * 2048;

        private static int _parCount = 4;
        
        static void Main(string[] args)
        {
            var begin = DateTime.UtcNow;

            if (!ParseArguments(args))
                return;
            
            if (_output == null)
            {
                var fileName = "unsorted.txt";
                _output = File.Create(fileName, 1024 * 1024 * 512);
            }

            var pars = new List<Task>(_parCount);
            for (int i = 0; i < _parCount; ++i)
            {
                pars.Add(Task.Run(() => GenerateBlockWrapper()));
            }

            Task.WaitAll(pars.ToArray());
            
            _output.Close();
            
            Console.WriteLine("Generating completed in {0}", DateTime.UtcNow - begin);

            Console.ReadKey();
        }
        
        private static bool ParseArguments(string[] args)
        {
            for (int i = 0; i < args.Length; ++i)
            {
                if (args[i].StartsWith("--"))
                {
                    if (args[i] == "--size")
                    {
                        ++i;
                        if (i >= args.Length)
                        {
                            Console.WriteLine("--size value is not defined");
                            return false;
                        }

                        int size;
                        var numberValid = int.TryParse(args[i], out size);
                        if (!numberValid)
                        {
                            Console.WriteLine("{0} is not valid number", args[i]);
                            return false;
                        }

                        _fileSize = size * 1024L * 1024;
                        continue;
                    }
                    
                    if (args[i] == "--par")
                    {
                        ++i;
                        if (i >= args.Length)
                        {
                            Console.WriteLine("--par value is not defined");
                            return false;
                        }

                        int parCount;
                        var numberValid = int.TryParse(args[i], out parCount);
                        if (!numberValid)
                        {
                            Console.WriteLine("{0} is not valid number", args[i]);
                            return false;
                        }

                        _parCount = parCount;
                    }
                    
                    if (args[i] == "--len")
                    {
                        ++i;
                        if (i >= args.Length)
                        {
                            Console.WriteLine("--len value is not defined");
                            return false;
                        }

                        int len;
                        var numberValid = int.TryParse(args[i], out len);
                        if (!numberValid)
                        {
                            Console.WriteLine("{0} is not valid number", args[i]);
                            return false;
                        }

                        _maxStringLength = len;
                    }
                }
                else
                {
                    _output = File.Create(args[i], 1024 * 1024 * 512);
                }
            }

            return true;
        }

        static void GenerateBlockWrapper()
        {
            GenerateBlock(BLOCK_SIZE, _fileSize / _parCount);
        }

        static void GenerateBlock(long blockSize, long taskLimit)
        {
            long totallyGenerated = 0;
            var block = new byte[blockSize + _maxStringLength + 64];
            int blockIndex = 0;
            Random rnd = new Random();

            var duplicatedBlock = new byte[_maxStringLength + 64];
            var duplicateLength = 0;

            for (;;)
            {
                bool dropDuplicate = rnd.Next() % 32 == 0;
                if (dropDuplicate)
                {
                    Array.Copy(duplicatedBlock, 0, block, blockIndex, duplicateLength);
                    blockIndex += duplicateLength;
                }

                var prevIndex = blockIndex;
                
                String numberString = rnd.Next().ToString();

                var numberBytes = Encoding.ASCII.GetBytes(numberString);
                Array.Copy(numberBytes, 0, block, blockIndex, numberBytes.Length);

                blockIndex += numberString.Length;
                totallyGenerated += numberString.Length;

                block[blockIndex] = (byte) '.';
                block[++blockIndex] = (byte) ' ';
                ++blockIndex;

                var stringPartSize = rnd.Next(3, _maxStringLength);

                byte[] stringPartArray = new byte[stringPartSize];
                
                
                rnd.NextBytes(stringPartArray);
                for (int i = 0; i < stringPartArray.Length; i++)
                    stringPartArray[i] = (byte) (stringPartArray[i] % 52 + (byte) 'A');
                
                stringPartArray[stringPartSize - 2] = (byte) '\n';
                stringPartArray[stringPartSize - 1] = (byte) '\r';

                Array.Copy(stringPartArray, 0, block, blockIndex, stringPartArray.Length);

                blockIndex += stringPartSize;

                totallyGenerated += stringPartSize;
                
                
                bool produceDuplicate = rnd.Next() % 32 == 0;
                if (produceDuplicate)
                {
                    var entryLength = blockIndex - prevIndex;
                    Array.Copy(block, prevIndex, duplicatedBlock, 0, entryLength);
                    duplicateLength = 0;
                }

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
