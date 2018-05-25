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
            var scanned1 = 0;
            while (array1[index1 + scanned1] != '.')
                ++scanned1;
            
            var scanned2 = 0;
            while (array1[index2 + scanned2] != '.')
                ++scanned2;

            if (scanned1 < scanned2)
                return -1;
            if (scanned1 > scanned2)
                return 1;

            while (array1[index1] != '.' && array2[index2] != '.')
            {
                var digit1 = array1[index1];
                var digit2 = array2[index2];
                
                if (digit1 < digit2)
                    return -1;
                else if (digit1 > digit2)
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