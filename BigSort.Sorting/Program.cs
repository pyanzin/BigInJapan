﻿using System;
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

        private static int ParCount = 1;

        private static byte[] parDelims;

        static void Main(string[] args)
        {
            GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;
            
            Start = DateTime.UtcNow;

            var argsCorrect = ParseArguments(args);

            if (!argsCorrect)
                return;
            
            if (_inputFile == null)
                _inputFile = new InputFile("unsorted.txt", 1024 * 1024 * 1024);

            if (_outputFileName == null)
                _outputFileName = "sorted.txt";

            InitialSortWrapper();
            
            if (_chunkFiles.Count > 1)
            {
                using (var output = new OutputFile(_outputFileName))
                {
                    var merger = new NWayMerger(_chunkFiles.ToArray(), output);
                    merger.Merge();
                }
            }
            else
            {
                var outputChunkName = _chunkFiles.Pop().Name;
                File.Move(outputChunkName, _outputFileName);
            }

            Console.WriteLine("Processing completed in {0}", (DateTime.UtcNow - Start));
            
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

                    if (_inputFile == null)
                        _inputFile = new InputFile(args[i], 1024 * 1024 * 1024);
                        
                    else if (_outputFileName == null)
                        _outputFileName = args[i];
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
        
        public static void InitialSortWrapper()
        {
            var sortStart = DateTime.UtcNow;
            while (!_inputFile.IsEnded)
            {
                GetChunkAndSort();
                Console.WriteLine(DateTime.UtcNow - sortStart);
                sortStart = DateTime.UtcNow;
            }
        }
        
        public static void GetChunkAndSort()
        {
            long partCount = InputFile.CHUNK_SIZE / (1024L * 1024 * 1024) + 1;
            var chunkParts = new byte[partCount][];
            var readParts = new long[partCount];

            var entryLists = new List<(int, int)>[ParCount];
            for (int i = 0; i < ParCount; i++)
            {
                entryLists[i] = new List<(int, int)>();
            }

            for (int partIndex = 0; partIndex < partCount; ++partIndex)
            {
                var (read, chunk) = GetInputChunk();

                if (parDelims == null)
                    FillDelims(chunk);
                
                if (read == 0)
                    break;

                chunkParts[partIndex] = chunk;
                readParts[partIndex] = read;

                var entry = 0;
                var i = entry;
                byte firstLetter = 0;
                while (i < read)
                {
                    var letter = chunk[i];
                    if (letter == '.' && firstLetter == 0)
                        firstLetter = chunk[i + 2];
                    if (letter == '\r')
                    {
                        int size = i - entry + 1;
                        for (int deli = 0; deli < parDelims.Length; ++deli)
                        {
                            if (firstLetter < parDelims[deli])
                            {
                                entryLists[deli].Add((entry, (partIndex << 16) | size));
                                break;
                            }
                        }

                        firstLetter = 0;
                        entry = i + 1;
                    }

                    ++i;
                }
            }

            var sortTasks = entryLists.Select(el => new Task(() => el.Sort((a, b) =>
            {
                int partIndexA = (int) (a.Item2 & 0xffff0000) >> 16;
                int partIndexB = (int) (b.Item2 & 0xffff0000) >> 16;

                byte[] bufferA = chunkParts[partIndexA];
                byte[] bufferB = chunkParts[partIndexB];

                return Entry.LessThan(bufferA, a.Item1, bufferB, b.Item1);

            }), TaskCreationOptions.LongRunning)).ToArray();

            foreach (var sortTask in sortTasks)
                sortTask.Start();

            Task.WaitAll(sortTasks);

            var outputFileName = GetFileName();

            using (var output = new OutputFile(outputFileName, 256 * 1024 * 1024))
            {
                foreach (var el in entryLists)
                    foreach (var e in el)
                    {
                        int partIndex = (int)(e.Item2 & 0xffff0000) >> 16;
                        output.WriteEntry(chunkParts[partIndex], e.Item1, e.Item2 & 0xffff);
                    }
            }
            
            PutAndContinue(new ChunkFile(outputFileName, readParts.Sum()));

            //SpawnMergeTask();
            
            GC.Collect();
        }

        private static void FillDelims(byte[] chunk)
        {
            var len = 2048;
            if (chunk.Length < len)
                len = chunk.Length;

            byte min = byte.MaxValue, max = byte.MinValue;
            
            for (int i = 0; i < len; i++)
            {
                if (chunk[i] == '\n' || chunk[i] == '\r')
                    continue;
                if (chunk[i] < min)
                    min = chunk[i];
                if (chunk[i] > max)
                    max = chunk[i];
            }

            var rangeSize = (max - min) / (ParCount);
            
            parDelims = new byte[ParCount];
            
            for (int i = 1; i < ParCount; ++i)
            {
                parDelims[i - 1] = (byte)(min + (i * rangeSize));
            }

            parDelims[ParCount - 1] = byte.MaxValue;
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
