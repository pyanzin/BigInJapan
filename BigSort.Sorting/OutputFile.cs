using System;
using System.IO;

namespace BigSort.Sorting
{
    public class OutputFile : IDisposable
    {
        public OutputFile(string fileName, int bufferSize = 1024 * 1024 * 128)
        {
            Out = new FileStream(fileName, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Write, bufferSize, false);
            _outputBuffer = new byte[bufferSize];
            _bufferSize = bufferSize;
        }

        private byte[] _outputBuffer;

        private int _bufferIndex;

        private int _bufferSize;

        public void WriteEntry(byte[] src, int pos, int size)
        {
            if (_bufferIndex + size > _bufferSize)
            {
                Out.Write(_outputBuffer, 0, _bufferIndex);
                Out.Write(src, pos, size);
                _bufferIndex = 0;
                GC.Collect();
            }
            else
            {
                Array.Copy(src, pos, _outputBuffer, _bufferIndex, size);
                _bufferIndex += size;
            }
        }

        public FileStream Out;
        
        public void Dispose()
        {
            Out.Dispose();
        }
    }
}