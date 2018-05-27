using System;
using System.IO;
using System.Text;
using System.Threading;

namespace BigSort.Sorting
{
    public class OutputFile : IDisposable
    {
        public OutputFile(string fileName, int bufferSize = 1024 * 1024 * 256)
        {
            Out = File.OpenWrite(fileName);
            _outputBuffer = new byte[bufferSize];
            _bufferSize = bufferSize;
            _outputThread = new Thread(WriteBuffer);
            _outputThread.Start();
        }

        private Thread _outputThread;

        private byte[] _internalBuffer;
        private byte[] _outputBuffer;

        private int _bufferIndex;
        private int _internalBufferIndex;

        private int _bufferSize;

        private bool _isDisposed = false;
        
        private object _bufferFreedLock = new object();
        private object _nextChunkReadyLock = new object();

        public void WriteEntry(byte[] src, int pos, int size)
        {
            if (_bufferIndex + size > _bufferSize)
            {
                lock (_bufferFreedLock)
                {
                    if (_internalBuffer != null)
                        Monitor.Wait(_bufferFreedLock);
                    lock (_nextChunkReadyLock)
                    {
                        _internalBuffer = _outputBuffer;
                        _internalBufferIndex = _bufferIndex;

                        _outputBuffer = new byte[_bufferSize];
                        _bufferIndex = 0;
                        SortingMain.CustomCopy(src, pos, _outputBuffer, 0, size);
                        _bufferIndex += size;

                        Monitor.Pulse(_nextChunkReadyLock);
                    }
                }
            }
            else
            {
                SortingMain.CustomCopy(src, pos, _outputBuffer, _bufferIndex, size);
                _bufferIndex += size;
            }
        }

        private void WriteBuffer()
        {
            lock (_nextChunkReadyLock)
            {
                for (;;)
                {
                    if (_internalBuffer == null)
                        Monitor.Wait(_nextChunkReadyLock);
                    lock (_bufferFreedLock)
                    {
                        Out.Write(_internalBuffer, 0, _internalBufferIndex);
                        _internalBuffer = null;
                        _internalBufferIndex = 0;

                        if (_isDisposed)
                        {
                            Out.Dispose();
                            return;
                        }

                        Monitor.Pulse(_bufferFreedLock);
                        
                    }
                    GC.Collect();
                }
            }
        }
        
        private FileStream Out;
        
        public void Dispose()
        {
            lock (_bufferFreedLock)
            {
                if (_internalBuffer != null)
                    Monitor.Wait(_bufferFreedLock);
                lock (_nextChunkReadyLock)
                {
                    if (_outputBuffer != null)
                    {
                        _internalBuffer = _outputBuffer;
                        _internalBufferIndex = _bufferIndex;
                    }

                    _isDisposed = true;
                    Monitor.Pulse(_nextChunkReadyLock);
                }
            }

            _outputThread.Join();
        }
    }
}