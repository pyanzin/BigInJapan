using System;
using System.IO;

namespace BigSort.Sorting
{
    public class InputFile : IDisposable
    {
        public const int CHUNK_SIZE = 1024 * 1024;
        
        public InputFile(string fileName)
        {
            In = File.OpenRead(fileName);
        }

        public FileStream In;

        public int GetNextChunk(byte[] array, int pos)
        {
            In.Seek(pos, SeekOrigin.Begin);
            var read = In.Read(array, pos, CHUNK_SIZE);
            return read;
        }

        public void Dispose()
        {
            In.Dispose();
        }
    }
}