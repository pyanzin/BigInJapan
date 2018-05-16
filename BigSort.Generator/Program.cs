using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigSort.Generator {
    class Program {

        static Random rnd = new Random();
        static void Main(string[] args)
        {
            var begin = DateTime.UtcNow;
            var fileName = "unsorted.txt";

            using (var file = File.Create(fileName))
            {
                for (int i = 0; i < 200000; i++)
                {
                    var entry = GenerateEntry();
                    file.Write(entry, 0, entry.Length);
                }
            }

            Console.WriteLine(DateTime.UtcNow - begin);
            Console.ReadKey();
        }

        static byte[] GenerateEntry()
        {
            var len = rnd.Next(64);
            var symbols = new byte[len];
            rnd.NextBytes(symbols);

            for (int i = 0; i < symbols.Length; i++)
            {
                symbols[i] = (byte)(symbols[i] % 52 + (byte)'A');
            }

            return Encoding.UTF8.GetBytes(string.Format("{0}. {1}\n\r", rnd.Next(), Encoding.UTF8.GetString(symbols)));
        }
    }
}
