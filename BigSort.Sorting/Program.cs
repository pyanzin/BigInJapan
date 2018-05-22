using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BigSort.Sorting {
    class SortingMain
    {
        private static DateTime Start;

        private static InputFile _inputFile;

        private static string _outputFileName;

        private static Int64 OriginalSize = 0;

        static void Main(string[] args)
        {
            Start = DateTime.UtcNow;

            var argsCorrect = ParseArguments(args);

            if (!argsCorrect)
                return;

            _inputFile = new InputFile("unsorted.txt");
            
            ThreadPool.QueueUserWorkItem(InitialSortWrapper);
            ThreadPool.QueueUserWorkItem(InitialSortWrapper);
            ThreadPool.QueueUserWorkItem(InitialSortWrapper);
            ThreadPool.QueueUserWorkItem(InitialSortWrapper);
            ThreadPool.QueueUserWorkItem(InitialSortWrapper);
            
            Console.ReadKey();
        }

        private static bool ParseArguments(string[] args)
        {
            for (int i = 0; i < args.Length; ++i)
            {
                if (args[i].StartsWith("--"))
                {
                    if (args[i] == "--buffer")
                    {
                        ++i;
                        if (i >= args.Length)
                        {
                            Console.WriteLine("Buffer value is not defined");
                            return false;
                        }

                        int buffer;
                        var numberValid = int.TryParse(args[i], out buffer);
                        if (!numberValid)
                        {
                            Console.WriteLine("{0} is not valid number", args[i]);
                            return false;
                        }

                        InputFile.CHUNK_SIZE = buffer * 1024 * 1024;

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
                        _inputFile = new InputFile(args[i]);
                    else if (_outputFileName == null)
                        _outputFileName = args[i];
                    else
                    {
                        Console.WriteLine("Unknown parameter '{0}'", args[i]);
                        return false;
                    }
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
            var (buffer, read) = GetInputChunk();

            while (buffer != null)
            {
                OriginalSize += read;
                
                var entries = new List<(int, int)>(1024 * 1024);

                var entry = 0;
                var i = entry;
                while (i < read)
                {
                    if (buffer[i] == '\r')
                    {
                        entries.Add((entry, i + 1));
                        entry = i + 1;
                    } 

                    ++i;
                }

                entries.Sort((a, b) => {
                    var i1 = a.Item1;
                    while (buffer[i1] != '.')
                        ++i1;
                    i1 += 2;

                    var j = b.Item1;
                    while (buffer[j] != '.')
                        ++j;
                    j += 2;

                    while (buffer[i1] != '\r' || buffer[j] != '\r')
                    {
                        if (buffer[i1] < buffer[j])
                            return -1;
                        else if (buffer[i1] > buffer[j])
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
                        output.WriteEntry(buffer, e.Item1, e.Item2 - e.Item1);
                    }
                }

                var hasChunksToMerge = PutAndContinue(new ChunkFile(outputFileName, read));
                
                if (hasChunksToMerge)
                    SpawnMergeTask();
                
                (buffer, read) = GetInputChunk();
            }
        }
        
        public static (byte[], int) GetInputChunk()
        {
            lock (_inputFileLock)
            {
                if (_inputFile.IsEnded)
                {
                    _inputFile.Dispose();
                    return (null, 0);
                }
                var buffer = new byte[InputFile.CHUNK_SIZE];
                var read = _inputFile.GetNextChunk(buffer);

                return (buffer, read);
            }
        }

        static object _fileNameLock = new object();
        private static int _fileNameCounter = 0;

        public static string GetFileName()
        {
            lock (_fileNameLock)
            {
                return $"output{_fileNameCounter++}.bin" +
                       $"";
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
