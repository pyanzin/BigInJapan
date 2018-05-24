using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace BigSort.Sorting
{
    public class NWayMerger
    {
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
            var frontEntries = new SortedSet<(int, int)>(new EntryComparer(_entryStreams));

            foreach (var stream in _entryStreams)
            {
                frontEntries.Add(stream.GetEntry());
            }

            while (frontEntries.Count > 0)
            {
                var smallest = frontEntries.FirstOrDefault();

                var streamIndex = (smallest.Item2 & 0xffff0000) >> 16;
                
                _output.WriteEntry(_entryStreams[streamIndex].Chunk, smallest.Item1, smallest.Item2 & 0xffff);
                frontEntries.RemoveWhere(x => x.Item1 == smallest.Item1 && x.Item2 == smallest.Item2);
                _entryStreams[streamIndex].Advance();

                if (_entryStreams[streamIndex].HasEntry)
                    frontEntries.Add(_entryStreams[streamIndex].GetEntry());
            }
        }
    }

    public class EntryComparer : Comparer<(int, int)>
    {
        private EntryStream[] _entryStreams;

        public EntryComparer(EntryStream[] entryStreams)
        {
            _entryStreams = entryStreams;
        }
        public override int Compare((int, int) a, (int, int) b)
        {
            int partIndexA = (int) (a.Item2 & 0xffff0000) >> 16;
            int partIndexB = (int) (b.Item2 & 0xffff0000) >> 16;

            byte[] bufferA = _entryStreams[partIndexA].Chunk;
            byte[] bufferB = _entryStreams[partIndexB].Chunk;

            return Entry.LessThan(bufferA, a.Item1, bufferB, b.Item1);
        }
    }
    public class EntryStream
    {
        private InputFile _file;

        public EntryStream(InputFile file, int id)
        {
            In = file;
            _id = id;
            Advance();
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
            return value;
        }

    }

}