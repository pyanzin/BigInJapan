using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BigSort.Sorting {
    class Program
    {
        static void Main(string[] args)
        {
            //_inputFile = new InputFile("unsorted.txt");
            
            var start = DateTime.UtcNow;
            using (var unsorted = new InputFile("unsorted.txt"))
            {
                while (!unsorted.IsEnded)
                {
                    var buffer = new byte[InputFile.CHUNK_SIZE];
                    var read = unsorted.GetNextChunk(buffer);

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

                    entries.Sort((a, b) => Entry.LessThan(buffer, a.Item1, buffer, b.Item1));

                    var outputFileName = GetFileName();
                    using (var output = new OutputFile(outputFileName))
                    {
                        foreach (var e in entries)
                        {
                            output.WriteEntry(buffer, e.Item1, e.Item2 - e.Item1);
                        }
                    }

                    PutChunkFile(new ChunkFile(outputFileName, read));
                }
            }
            
            Merge();
            
            Console.WriteLine(_chunkFiles.Pop().Name);
            
            Console.WriteLine(DateTime.UtcNow - start);

            Console.ReadKey();
        }

        private static InputFile _inputFile;
        private static object _inputFileLock = new object();
        
        public (byte[], int) GetInputChunk()
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
                return $"output{_fileNameCounter++}.txt";
            }
        }

        public static void Merge()
        {
            var (hasChunks, chunk1, chunk2) = PutAndContinue(null);

            while (hasChunks)
            {
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

                (hasChunks, chunk1, chunk2) = PutAndContinue(new ChunkFile(outputName, chunk1.Size + chunk2.Size));
            }
        }

        private static object _chunkFileLock = new object();
        
        private static Stack<ChunkFile> _chunkFiles = new Stack<ChunkFile>();

        public static (bool, ChunkFile, ChunkFile) PutAndContinue(ChunkFile chunkFile)
        {
            lock (_chunkFileLock)
            {
                if (chunkFile != null)
                    _chunkFiles.Push(chunkFile);

                _chunkFiles = new Stack<ChunkFile>(_chunkFiles.OrderByDescending(x => x.Size));

                if (_chunkFiles.Count < 2)
                    return (false, null, null);

                return (true, _chunkFiles.Pop(), _chunkFiles.Pop());
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
    }
}
