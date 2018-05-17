namespace BigSort.Sorting
{
    public static class Merger
    {
        public static void Merge(InputFile in1, InputFile in2, OutputFile output)
        {
            var chunk1 = new byte[InputFile.CHUNK_SIZE];
            var read1 = in1.GetNextChunk(chunk1, 0);
            
            var chunk2 = new byte[InputFile.CHUNK_SIZE];
            var read2 = in1.GetNextChunk(chunk2, 0);

            var entryPos1 = 0;
            var entryPos2 = 0;

            begin:
            var nextEntryPos1 = entryPos1;

            for (;;)
            {
                ++nextEntryPos1;

                if (entryPos1 >= chunk1.Length)
                {
                    read1 = in1.GetNextChunk(chunk1, entryPos1);
                    nextEntryPos1 = nextEntryPos1 - entryPos1;
                    entryPos1 = 0;
                }
                
                if (chunk1[nextEntryPos1] == '\r')
                {
                    nextEntryPos1 += 1;
                    break;
                }
            }

        }
    }
}