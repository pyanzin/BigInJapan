using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace BigSort.Sorting
{
    public class NWayMerger
    {
        private InputFile[] _chunks;
        private OutputFile _output;
        private EntryStream[] _entryStreams;

        public NWayMerger(ChunkFile[] chunkFiles, OutputFile outputFile)
        {
            _entryStreams = new EntryStream[chunkFiles.Length];
            for (int i = 0; i < chunkFiles.Length; ++i)
            {
                _entryStreams[i] = new EntryStream(new InputFile(chunkFiles[i].Name), i);

            }
            _output = outputFile;
        }

        public void Merge()
        {
            var frontEntries = _entryStreams.Select(s => s.GetEntry()).ToList();

            while (frontEntries.Count > 0)
            {
                frontEntries.Sort((a, b) =>
                {
                    int partIndexA = (int) (a.Item2 & 0xffff0000) >> 16;
                    int partIndexB = (int) (b.Item2 & 0xffff0000) >> 16;

                    byte[] bufferA = _entryStreams[partIndexA].Chunk;
                    byte[] bufferB = _entryStreams[partIndexB].Chunk;

                    return -Entry.LessThan(bufferA, a.Item1, bufferB, b.Item1);
                });
                
                var smallest = frontEntries[frontEntries.Count - 1];

                var streamIndex = (smallest.Item2 & 0xffff0000) >> 16;
                
                _output.WriteEntry(_entryStreams[streamIndex].Chunk, smallest.Item1, smallest.Item2 & 0xffff);

                if (_entryStreams[streamIndex].HasEntry)
                    frontEntries[frontEntries.Count - 1] = _entryStreams[streamIndex].GetEntry();
                else
                    frontEntries.RemoveAt(frontEntries.Count - 1);
            }
        }
    }

    public class EntryStream
    {
        private InputFile _file;

        public EntryStream(InputFile file, int id)
        {
            In = file;
            (Read, Chunk) = In.GetNextChunk();
            _id = id;
        }

        public InputFile In { get; set; }

        public byte[] Chunk;

        public int Read;

        public int Pos;

        public int Next;

        public bool HasEntry = true;
        
        private int _id;

        public void Advance()
        {
            if (Next >= Read)
            {
                if (In.IsEnded)
                {
                    HasEntry = false;
                    return;
                }

                (Read, Chunk) = In.GetNextChunk();

                Pos = 0;
                Next = 0;
            }

            Pos = Next;
            while (Chunk[Next++] != '\r')
                ;
        }

        public (int, int) GetEntry()
        {
            var value = (Pos, (_id << 16) | Next - Pos);
            Advance();
            return value;
        }

    }

}