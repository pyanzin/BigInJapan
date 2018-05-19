namespace BigSort.Sorting
{
    public class Merger
    {
        public Merger(InputFile inLeft, InputFile inRight, OutputFile output)
        {
            InLeft = inLeft;
            InRight = inRight;
            Out = output;
            
            ChunkLeft = new byte[InputFile.CHUNK_SIZE];
            ChunkRight = new byte[InputFile.CHUNK_SIZE];

            ReadLeft = inLeft.GetNextChunk(ChunkLeft);
            ReadRight = inRight.GetNextChunk(ChunkRight);
            
            AdvanceLeft();
            AdvanceRight();
        }

        public InputFile InLeft { get; set; }
        public InputFile InRight { get; set; }
        public OutputFile Out { get; set; }

        public byte[] ChunkLeft { get; set; }
        public byte[] ChunkRight { get; set; }

        public int ReadLeft;
        public int ReadRight;

        public int leftPos;
        public int rightPos;

        public int leftNext;
        public int rightNext;

        public bool LeftHasEntry = true;
        public bool RightHasEntry = true;

        public void AdvanceLeft()
        {
            if (leftNext >= ReadLeft)
            {
                if (InLeft.IsEnded)
                {
                    LeftHasEntry = false;
                    return;
                }
                ReadLeft = InLeft.GetNextChunk(ChunkLeft);
                
                leftPos = 0;
                leftNext = 0;
            }
            leftPos = leftNext;
            while (ChunkLeft[leftNext++] != '\r')
                ;
        }
        
        public void AdvanceRight()
        {
            if (rightNext >= ReadRight)
            {
                if (InRight.IsEnded)
                {
                    RightHasEntry = false;
                    return;
                }
                ReadRight = InRight.GetNextChunk(ChunkRight);
                
                rightPos = 0;
                rightNext = 0;
            }
            rightPos = rightNext;
            while (ChunkRight[rightNext++] != '\r')
                ;
        }

        public void WriteRestLeft()
        {
            if (LeftHasEntry)
                Out.WriteEntry(ChunkLeft, leftPos, ReadLeft - leftPos);
        }
        
        public void WriteRestRight()
        {
            if (RightHasEntry)
                Out.WriteEntry(ChunkRight, rightPos, ReadRight - rightPos);
        }
        
        public void Merge()
        {
            while (LeftHasEntry && RightHasEntry)
            {
                var left = Entry.MakeString(ChunkLeft, leftPos);
                var right = Entry.MakeString(ChunkRight, rightPos);
                
                var leftIsLess = Entry.LessThan(ChunkLeft, leftPos, ChunkRight, rightPos) == -1;

                if (leftIsLess)
                {
                    Out.WriteEntry(ChunkLeft, leftPos, leftNext - leftPos);
                    AdvanceLeft();
                } else
                {
                    Out.WriteEntry(ChunkRight, rightPos, rightNext - rightPos);
                    AdvanceRight();
                }
            }

            WriteRestLeft();
            WriteRestRight();

        }


    }
}