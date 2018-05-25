using System;
using System.Text;

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

            var strResult = StringLess(src1, i, src2, j);

            if (strResult != 0)
                return strResult;
            
            var n1 = pos1;
            while (src1[n1] != '.')
                ++n1;
            
            var n2 = pos2;
            while (src1[n2] != '.')
                ++n2;

            if (n1 - pos1 < n2 - pos2)
                return -1;
            else if (n1 - pos1 > n2 - pos2)
                return 1;
            return NumberLess(src1, pos1, src2, pos2);

        }

        public static int StringLess(byte[] array1, int index1, byte[] array2, int index2)
        {
            while (array1[index1] != '\r' && array2[index2] != '\r')
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

        public static int NumberLess(byte[] array1, int index1, byte[] array2, int index2)
        {
            while (array1[index1] != '.' && array2[index2] != '.')
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
        
        public static string MakeString(byte[] array, int index)
        {
            var i = index;
            while (array[i++] != '\r')
                ;
            ++i;

            var copy = new byte[i - index];

            Array.Copy(array, index, copy, 0, i - index);

            return Encoding.ASCII.GetString(copy);
        }
    }
}