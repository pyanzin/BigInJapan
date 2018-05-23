using System;
using System.IO;

namespace BigSort.Sorting
{
    public class InputFile : IDisposable
    {
        public static long CHUNK_SIZE = 1024L * 1024 * 1024;

        public long NextPos = 0;

        public bool IsEnded = false;
        
        public InputFile(string fileName)
        {
            In = File.OpenRead(fileName);
        }

        public FileStream In;

        public int GetNextChunk(byte[] array)
        {
            In.Seek(NextPos, SeekOrigin.Begin);
            var read = In.Read(array, 0, array.Length);
            if (read < array.Length)
            {
                IsEnded = true;
                return read;
            }

            var lastCrIndex = read;

            while (array[--lastCrIndex] != '\r')
                ;

            NextPos = NextPos + lastCrIndex + 1;
            return lastCrIndex + 1;
        }

        public void Dispose()
        {
            In.Dispose();
        }
    }
}