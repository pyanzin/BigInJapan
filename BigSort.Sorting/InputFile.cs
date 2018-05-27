using System;
using System.IO;
using System.Threading;

namespace BigSort.Sorting
{
    public class InputFile : IDisposable
    {
        public long NextPos = 0;

        public bool IsEnded = false;
        
        public InputFile(string fileName, int chunkSize = 256 * 1024 * 1024)
        {
            _chunkSize = chunkSize;
            In = File.OpenRead(fileName);
            new Thread(ReadNextChunk).Start();
        }

        private int _chunkSize;

        public FileStream In;

        private byte[] _nextChunk;
        private int _nextChunkRead;

        private bool _readingCompleted = false;

        private object _nextChunkFreedLock = new object();
        private object _nextChunkReadyLock = new object();

        private void ReadNextChunk()
        {
            lock (_nextChunkFreedLock)
            {
                for (;;)
                {
                    if (_nextChunk != null)
                        Monitor.Wait(_nextChunkFreedLock);
                    lock (_nextChunkReadyLock)
                    {
                        _nextChunk = new byte[_chunkSize];

                        In.Seek(NextPos, SeekOrigin.Begin);
                        _nextChunkRead = In.Read(_nextChunk, 0, _chunkSize);
                        
                        if (_nextChunkRead < _chunkSize)
                        {
                            In.Dispose();
                            _readingCompleted = true;
                            Monitor.Pulse(_nextChunkReadyLock);
                            return;
                        }

                        var lastCrIndex = _nextChunkRead;

                        while (_nextChunk[--lastCrIndex] != '\r')
                            ;

                        NextPos = NextPos + lastCrIndex + 1;
                        _nextChunkRead = lastCrIndex + 1;

                        Monitor.Pulse(_nextChunkReadyLock);
                    }
                }
            }
        }

        public (int, byte[]) GetNextChunk()
        {
            lock (_nextChunkReadyLock)
            {
                if (_nextChunk == null)
                    Monitor.Wait(_nextChunkReadyLock);
                lock (_nextChunkFreedLock)
                {
                    var nextChunkTuple = (_nextChunkRead, _nextChunk);
                    _nextChunk = new byte[0];
                    _nextChunkRead = 0;

                    if (_readingCompleted)
                        IsEnded = true;

                    Monitor.Pulse(_nextChunkFreedLock);
                    return nextChunkTuple;
                }
            }
        }

        public void Dispose()
        {
            
        }
    }
}