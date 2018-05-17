using System;
using System.IO;

namespace BigSort.Sorting
{
    public class OutputFile : IDisposable
    {
        public OutputFile(string fileName)
        {
            Out = File.OpenWrite(fileName);
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