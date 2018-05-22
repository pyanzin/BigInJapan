using System;
using System.IO;

namespace BigSort.Sorting
{
    public class OutputFile : IDisposable
    {
        public OutputFile(string fileName)
        {
            Out = new FileStream(fileName, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Write, 1024 * 1024 * 128, false);
        }

        public void WriteEntry(byte[] src, int pos, int size)
        {
            Out.Write(src, pos, size);
        }

        public FileStream Out;
        
        public void Dispose()
        {
            Out.Dispose();
        }
    }
}