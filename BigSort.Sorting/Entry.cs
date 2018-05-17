namespace BigSort.Sorting
{
    public static class Entry
    {
        public static int LessThan(byte[] src1, int pos1, byte[] src2, int pos2)
        {
            var i = pos1;
            while (src1[i] != '.')
                ++i;
            i += 2;

            var j = pos2;
            while (src2[j] != '.')
                ++j;
            j += 2;

            return StringLess(src1, i, src2, j);
        }

        public static int StringLess(byte[] array1, int index1, byte[] array2, int index2)
        {
            while (array1[index1] != '\r' || array2[index2] != '\r')
            {
                if (array1[index1] < array2[index2])
                    return -1;
                else if (array1[index1] > array2[index2])
                    return 1;
                ++index1;
                ++index2;
            }
            return 0;
        }
    }
}