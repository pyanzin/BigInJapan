using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BigSort.Sorting {
    class SortingMain
    {
        private static DateTime Start;

        private static InputFile _inputFile;

        private static string _outputFileName;

        private static long OriginalSize = 0;

        private static int ParCount = 1;

        static void Main(string[] args)
        {
            GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;
            
            Start = DateTime.UtcNow;

            _inputFile = new InputFile("unsorted.txt", (int)InputFile.CHUNK_SIZE);

            var argsCorrect = ParseArguments(args);

            if (!argsCorrect)
                return;


            for (int i = 0; i < ParCount; ++i)
                ThreadPool.QueueUserWorkItem(InitialSortWrapper);
            
            Console.ReadKey();
        }

        private static bool ParseArguments(string[] args)
        {
            for (int i = 0; i < args.Length; ++i)
            {
                if (args[i].StartsWith("--"))
                {
                    if (args[i] == "--buf")
                    {
                        ++i;
                        if (i >= args.Length)
                        {
                            Console.WriteLine("--buffer value is not defined");
                            return false;
                        }

                        int buffer;
                        var numberValid = int.TryParse(args[i], out buffer);
                        if (!numberValid)
                        {
                            Console.WriteLine("{0} is not valid number", args[i]);
                            return false;
                        }

                        InputFile.CHUNK_SIZE = buffer * 1024L * 1024;
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

                        ParCount = parCount;
                    }
                }
                else
                {
                    if (!File.Exists(args[i]))
                    {
                        Console.WriteLine("File {0} not found", args[i]);
                        return false;
                    }

                    //if (_inputFile == null)
                        _inputFile = new InputFile(args[i]);
                       // _outputFileName = args[i];
                    //else if (_outputFileName == null)
                    //else
                    //{
                    //    Console.WriteLine("Unknown parameter '{0}'", args[i]);
                     //   return false;
                    //}
                }
            }

            return true;
        }

    private static object _inputFileLock = new object();

        public static void SpawnMergeTask()
        {
            ThreadPool.QueueUserWorkItem(MergeWrapper);
        }

        public static void MergeWrapper(object threadContext)
        {
            Merge();
        }
        
        public static void InitialSortWrapper(object threadContext)
        {
            GetChunkAndSort();
        }
        
        public static void GetChunkAndSort()
        {
            long partCount = InputFile.CHUNK_SIZE / (1024L * 1024 * 1024) + 1;
            long partSize = InputFile.CHUNK_SIZE / partCount;
            var chunkParts = new byte[partCount][];
            var readParts = new long[partCount];
            
            var entries = new List<(int, int)>(1024 * 1024);

            for (int partIndex = 0; partIndex < partCount; ++partIndex)
            {
                var (read, chunk) = GetInputChunk();
                
                OriginalSize += read;

                if (read == 0)
                    break;

                chunkParts[partIndex] = chunk;
                readParts[partIndex] = read;

                var entry = 0;
                var i = entry;
                while (i < read)
                {
                    if (chunk[i] == '\r')
                    {
                        int size = i - entry + 1;
                        entries.Add((entry, (partIndex << 16) | size));
                        entry = i + 1;
                    }

                    ++i;
                }
            }
            
            entries.Sort((a, b) =>
            {
                int partIndexA = (int)(a.Item2 & 0xffff0000) >> 16;
                int partIndexB = (int)(b.Item2 & 0xffff0000) >> 16;

                byte[] bufferA = chunkParts[partIndexA];
                byte[] bufferB = chunkParts[partIndexB];
                
                var i1 = a.Item1;
                while (bufferA[i1] != '.')
                    ++i1;
                i1 += 2;

                var j = b.Item1;
                while (bufferB[j] != '.')
                    ++j;
                j += 2;

                while (bufferA[i1] != '\r' || bufferB[j] != '\r')
                {
                    if (bufferA[i1] < bufferB[j])
                        return -1;
                    else if (bufferA[i1] > bufferB[j])
                        return 1;
                    ++i1;
                    ++j;
                }

                return 0;
            });

            var outputFileName = GetFileName();

            using (var output = new OutputFile(outputFileName))
            {
                foreach (var e in entries)
                {
                    int partIndex = (int)(e.Item2 & 0xffff0000) >> 16;
                    output.WriteEntry(chunkParts[partIndex], e.Item1, e.Item2 & 0xffff);
                }
            }
            
            PutAndContinue(new ChunkFile(outputFileName, readParts.Sum()));

            SpawnMergeTask();
            
            GC.Collect();
        }
        
        public static (int, byte[]) GetInputChunk()
        {
            lock (_inputFileLock)
            {
                return _inputFile.GetNextChunk();
            }
        }

        public static bool InputIsEnded()
        {
            lock (_inputFileLock)
            {
                return _inputFile.IsEnded;
            }
        }

        static object _fileNameLock = new object();
        private static int _fileNameCounter = 0;

        public static string GetFileName()
        {
            lock (_fileNameLock)
            {
                return $"output{_fileNameCounter++}.bin";
            }
        }

        public static void Merge()
        {
            var (hasChunks, chunk1, chunk2) = GetChunks();
            if (!hasChunks)
                return;

            var outputName = GetFileName();

            using (var chunkFile1 = new InputFile(chunk1.Name))
            {
                using (var chunkFile2 = new InputFile(chunk2.Name))
                {
                    using (var outputFile = new OutputFile(outputName))
                    {
                        var merger = new Merger(chunkFile1, chunkFile2, outputFile);
                        merger.Merge();
                    }
                }
            }

            hasChunks = PutAndContinue(new ChunkFile(outputName, chunk1.Size + chunk2.Size));
            if (hasChunks)
                SpawnMergeTask();
            
            File.Delete(chunk1.Name);
            File.Delete(chunk2.Name);   
            
            GC.Collect();
        }

        private static object _chunkFileLock = new object();
        
        private static Stack<ChunkFile> _chunkFiles = new Stack<ChunkFile>();

        public static bool PutAndContinue(ChunkFile chunkFile)
        {
            lock (_chunkFileLock)
            {
                if (chunkFile != null)
                    _chunkFiles.Push(chunkFile);

                _chunkFiles = new Stack<ChunkFile>(_chunkFiles.OrderByDescending(x => x.Size));
                
                if (Math.Abs(chunkFile.Size - OriginalSize) < 1024 * 1024)
                    Console.WriteLine("Processing completed in :" + (DateTime.UtcNow - Start));

                if (_chunkFiles.Count < 2)
                    return false;

                return true;
            }
        }

        public static void PutChunkFile(ChunkFile chunkFile)
        {
            lock (_chunkFileLock)
            {
                if (chunkFile != null)
                    _chunkFiles.Push(chunkFile);
                
                _chunkFiles = new Stack<ChunkFile>(_chunkFiles.OrderByDescending(x => x.Size));
            }
        }

        public static (bool, ChunkFile, ChunkFile) GetChunks()
        {
            lock (_chunkFileLock)
            {
                if (_chunkFiles.Count < 2)
                    return (false, null, null);

                return (true, _chunkFiles.Pop(), _chunkFiles.Pop());
            }
        }
    }
}
