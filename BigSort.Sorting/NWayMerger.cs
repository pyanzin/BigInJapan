﻿using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace BigSort.Sorting
{
    public class NWayMerger
    {
        private OutputFile _output;
        private EntryStream[] _entryStreams;
        private (int, int)[] _frontEntries;
        private EntryComparer _comparer;

        public NWayMerger(ChunkFile[] chunkFiles, OutputFile outputFile)
        {
            long inputFileBuffer = (long)(BigSort.ChunkSize / chunkFiles.Length * 0.4);
            if (inputFileBuffer > BigSort.OneGb)
                inputFileBuffer = BigSort.OneGb;
            
            _entryStreams = new EntryStream[chunkFiles.Length];
            for (int i = 0; i < chunkFiles.Length; ++i)
            {
                _entryStreams[i] = new EntryStream(new InputFile(chunkFiles[i].Name, (int)inputFileBuffer), i);

            }
            _output = outputFile;
        }

        public void Merge()
        {
            var frontList = _entryStreams.Select(e => e.GetEntry()).ToList();

            _comparer = new EntryComparer(_entryStreams);
            frontList.Sort(_comparer);

            _frontEntries = frontList.ToArray();

            while (_frontEntries.Length > 0)
            {
                var smallest = _frontEntries[0];

                var streamIndex = (smallest.Item2 & 0xffff0000) >> 16;

                var entryStream = _entryStreams[streamIndex];
                
                _output.WriteEntry(entryStream.Chunk, smallest.Item1, smallest.Item2 & 0xffff);
                
                entryStream.Advance();

                if (entryStream.HasEntry)
                {
                    _frontEntries[0] = entryStream.GetEntry();
                    Heapify(0);
                }
                else
                {
                    var newFrontEntries = _frontEntries.ToList();
                    newFrontEntries.RemoveAt(0);
                    newFrontEntries.Sort(_comparer);
                    _frontEntries = newFrontEntries.ToArray();
                }
            }
        }

        private void Heapify(int i)
        {
            int l = 2*i + 1;
            int r = 2*i + 2;
            int smallest = i;

            if (l < _frontEntries.Length && _comparer.Compare(_frontEntries[l], _frontEntries[i]) == -1)
                smallest = l;
            if (r < _frontEntries.Length && _comparer.Compare(_frontEntries[r], _frontEntries[smallest]) == -1)
                smallest = r;
            
            if (smallest != i)
            {
                var tmp = _frontEntries[i];
                _frontEntries[i] = _frontEntries[smallest];
                _frontEntries[smallest] = tmp;
                Heapify(smallest);
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
        public EntryStream(InputFile file, int id)
        {
            In = file;
            _streamId = id;
            Advance();
        }

        private InputFile In { get; set; }

        public byte[] Chunk;

        private int _read;

        private int _pos;

        private int _next;

        public bool HasEntry = true;
        
        private readonly int _streamId;

        public void Advance()
        {
            if (_next >= _read)
            {
                if (In.IsEnded)
                {
                    HasEntry = false;
                    return;
                }

                (_read, Chunk) = In.GetNextChunk();

                _pos = 0;
                _next = 0;
            }

            _pos = _next;
            while (Chunk[_next++] != '\r')
                ;
        }

        public (int, int) GetEntry()
        {
            var value = (_pos, (_streamId << 16) | _next - _pos);
            return value;
        }

    }

}