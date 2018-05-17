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
        private static string testDataRaw =
            @"1388810681. BHQJNiMtN]NfEKrE_S^g[QDt\]nTjYsHfMfnDhtoOVeeSUTaPWSiseabEM
983989516. ljKjZCplVEqgT_YE`SaHJNtPCJJBaDjhGSDUKkGLUYGLZ\VP
1436191077. rf`WT[flcPnoHSKJ]A\tnfHVcbGhjXijiEBtHdpY`TIlO[ReqLpUhJ_I[KdcPr
1549901064. XDBtMLCmBtP[^QCIPSg^YUC`[iZmqIIbSmPLVOXfJCOenD`K
1553680138. SnKTdCBWZTcGqmkcDaqgcBorQVFNcSILdQgLWsINKseAssCVk
2103290073. oNSiQpJtOndeDLXS]C_MFY`\eNopRPaSXSJrR\VLJokPXObj\]WTLokW_gADB
1558935786. oNMIarWBA]PtG^LEV\FTfjNHYXhS
791379718. SGpNlCGCqlB[VgmpdiOb`YqjlFqF`EGDWZJEDjcJoP_JOLT[K^eotmlkJFVg
1537432563. WhbrtqYhlnGFdLnd\abVoCQLDZbTooPmCrng\RHTsXgk
741503760. J\mW[fEaBTTSkZOEim[KDbOHHGVNTXMZYNQBXYU[hRlZOFWbFn_jMZcXNeOU
1572607420. N_Don]JI^QXpWfkfisdhE[jWMs[jecFAJqENVULjYnLORMsnaXmM
2041459035. ccfb^lCPTlGXgWaYQThjPGP[AAq^ENjnPfKR]
188518109. QhE[PDo[]MIDYZlI\TG]]oUGGBNHqIKp]]XCHapXGZU
1959379422. FNQY]csSHKR
";

        static void Main(string[] args)
        {
            var start = DateTime.UtcNow;
            using (var unsorted = File.OpenRead("unsorted.txt"))
            {
                var buffer = new byte[1024 * 1024 * 64];
                var read = unsorted.Read(buffer, 0, buffer.Length);

                var entries = new List<(int, int)>(1024 * 1024);

                var entry = 0;
                var i = entry;
                while (i < read)
                {
                    if (buffer[i] == '\r')
                    {
                        ++i;
                        entries.Add((entry, i));
                        ++i;
                        entry = i;
                    }
                    ++i;
                }
                
                entries.Add((entry, i));

                var res = Entry.LessThan(buffer, entries[0].Item1, buffer, entries[0].Item1);
                
                entries.Sort((a, b) => Entry.LessThan(buffer, a.Item1, buffer, b.Item1));

                using (var output = new OutputFile("sorted.txt"))
                {
                    foreach (var e in entries)
                    {
                        output.WriteEntry(buffer, e.Item1, e.Item2 - e.Item1);
                    }
                }
            }
            
            Console.WriteLine(DateTime.UtcNow - start);

            Console.ReadKey();
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

            if (!hasChunks)
                return;
            
            // ...merge here...
            
            // ...delete old chunks...
            
            // ...ask for next...
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
    }
}
