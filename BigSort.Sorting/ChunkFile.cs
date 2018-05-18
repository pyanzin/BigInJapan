namespace BigSort.Sorting
{
    public class ChunkFile
    {
        public ChunkFile(string fileName, long size)
        {
            Name = fileName;
            Size = size;
        }

        public string Name { get; set; }
        public long Size { get; set; }
    }
}