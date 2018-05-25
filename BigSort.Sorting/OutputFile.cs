using System;
using System.IO;
using System.Threading;

namespace BigSort.Sorting
{
    public class OutputFile : IDisposable
    {
        public OutputFile(string fileName, int bufferSize = 1024 * 1024 * 256)
        {
            Out = new FileStream(fileName, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Write, bufferSize, false);
            _outputBuffer = new byte[bufferSize];
            _bufferSize = bufferSize;
            new Thread(WriteBuffer).Start();
        }

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
                lock (_nextChunkReadyLock)
                {
                    lock (_bufferFreedLock)
                    {
                        if (_internalBuffer != null)
                            Monitor.Wait(_bufferFreedLock);
                        _internalBuffer = _outputBuffer;
                        _internalBufferIndex = _bufferIndex;
                        
                        _outputBuffer = new byte[_bufferSize];
                        _bufferIndex = 0;
                        Array.Copy(src, pos, _outputBuffer, 0, size);
                        
                        Monitor.Pulse(_nextChunkReadyLock);
                    }
                }
            }
            else
            {
                Array.Copy(src, pos, _outputBuffer, _bufferIndex, size);
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
                        if (_isDisposed)
                            return;
                        Out.Write(_internalBuffer, 0, _internalBufferIndex);
                        _internalBuffer = null;
                        _internalBufferIndex = 0;
                        Monitor.Pulse(_bufferFreedLock);
                        
                    }
                    GC.Collect();
                }
            }
            

        }
        
        private FileStream Out;
        
        public void Dispose()
        {
            lock (_nextChunkReadyLock)
            {
                if (_outputBuffer != null)
                    Out.Write(_outputBuffer, 0, _bufferIndex);
                _isDisposed = true;
                Monitor.Pulse(_nextChunkReadyLock);
                Out.Dispose();
            }
        }
    }
}