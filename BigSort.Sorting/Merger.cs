namespace BigSort.Sorting
{
    public class Merger
    {
        public Merger(InputFile inLeft, InputFile inRight, OutputFile output)
        {
            InLeft = inLeft;
            InRight = inRight;
            Out = output;
            
            ChunkLeft = new byte[1024 * 1024 * 512];
            ChunkRight = new byte[1024 * 1024 * 512];

            ReadLeft = inLeft.GetNextChunk(ChunkLeft);
            ReadRight = inRight.GetNextChunk(ChunkRight);
            
            AdvanceLeft();
            AdvanceRight();
        }

        public InputFile InLeft { get; set; }
        public InputFile InRight { get; set; }
        public OutputFile Out { get; set; }

        public byte[] ChunkLeft;
        public byte[] ChunkRight;

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
            {
                Out.WriteEntry(ChunkLeft, leftPos, ReadLeft - leftPos);
                if (!InLeft.IsEnded)
                {
                    ReadLeft = InLeft.GetNextChunk(ChunkLeft);
                    Out.WriteEntry(ChunkLeft, 0, ReadLeft);
                }
            }
        }
        
        public void WriteRestRight()
        {
            if (RightHasEntry)
            {
                Out.WriteEntry(ChunkRight, rightPos, ReadRight - rightPos);
                if (!InRight.IsEnded)
                {
                    ReadRight = InRight.GetNextChunk(ChunkRight);
                    Out.WriteEntry(ChunkRight, 0, ReadRight);
                }
            }
        }
        
        public void Merge()
        {
            while (LeftHasEntry && RightHasEntry)
            {
                //var left = Entry.MakeString(ChunkLeft, leftPos);
                //var right = Entry.MakeString(ChunkRight, rightPos);

                var i = leftPos;
                while (ChunkLeft[i] != '.')
                    ++i;
                i += 2;

                var j = rightPos;
                while (ChunkRight[j] != '.')
                    ++j;
                j += 2;

                if (Entry.StringLess(ChunkLeft, i, ChunkRight, j) == -1)
                {
                    Out.Out.Write(ChunkLeft, leftPos, leftNext - leftPos);
                    AdvanceLeft();
                } else
                {
                    Out.Out.Write(ChunkRight, rightPos, rightNext - rightPos);
                    AdvanceRight();
                }
            }

            WriteRestLeft();
            WriteRestRight();

        }


    }
}