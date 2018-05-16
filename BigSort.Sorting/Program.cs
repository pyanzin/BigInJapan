using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
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
            using (var unsorted = File.OpenRead("unsorted.txt"))
            {
                var raw = new byte[20000000];
                unsorted.Read(raw, 0, 40000);

                var entries = new List<Entry>();
                Entry? entry = new Entry(raw, 0);

                do
                {
                    entries.Add(entry.Value);
                } while ((entry = entry.Value.ScanToNext()) != null);

                entries.Sort((a, b) => a.LessThan(b));

                for (int i = 0; i < entries.Count; ++i)
                {
                    var count = 0;
                    if (i == entries.Count - 1)
                        count = raw.Length - entries[i].Index;
                    else
                        count = entries[i + 1].Index - entries[i].Index;
                    Console.WriteLine(Encoding.ASCII.GetString(entries[i].Raw, entries[i].Index, count));
                }

                var output = new MemoryStream();
            }

            Console.ReadKey();
        }
    }

    struct Entry
    {
        public byte[] Raw;
        public int Index;

        public Entry(byte[] rawArray, int index)
        {
            Raw = rawArray;
            Index = index;
        }

        public Entry? ScanToNext()
        {
            var index = Index;
            while (index < Raw.Length && Raw[index] != '\n')
                ++index;
            index += 2;

            if (index >= Raw.Length)
                return null;

            return new Entry(Raw, index);
        }
        public int LessThan(Entry other)
        {
            var i = Index;
            while (Raw[i] != '.')
                ++i;
            i += 2;

            var j = other.Index;
            while (other.Raw[j] != '.')
                ++j;
            j += 2;

            return StringLess(Raw, i, other.Raw, j);
        }

        public int StringLess(byte[] array1, int index1, byte[] array2, int index2)
        {
            while (array1[index1] != '\n')
            {
                if (array1[index1++] < array2[index2])
                    return -1;
                else if (array1[index1++] > array2[index2])
                    return 1;
            }
            return 0;
        }
    }
}
