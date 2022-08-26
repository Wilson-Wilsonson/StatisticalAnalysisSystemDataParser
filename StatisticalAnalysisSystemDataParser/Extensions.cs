using System;
using System.Collections.Generic;
using System.Linq;

namespace StatisticalAnalysisSystemDataParser
{
    public  static class Extensions
    {
        public static int GetInteger(this byte[] input, int index, int length)
        {
            var byteList = new List<byte>();
            byteList.AddRange(input.Skip(index).Take(length));

            if (length >= 4)
                return BitConverter.ToInt32(byteList.ToArray(), 0);

            for (var i = 0; i < 4 - length; i++) // BitConverter will not properly convert byte arrays smaller than 4
            {
                byteList.Add(0);
            }

            return BitConverter.ToInt32(byteList.ToArray(), 0);
        }

        public static string GetString(this byte[] input, int index, int length)
        {
            return System.Text.Encoding.ASCII.GetString(input, index, length);
        }

        public static double GetDouble(this byte[] input, int index, int length)
        {
            return BitConverter.ToDouble(input.Skip(index).Take(length).ToArray(), 0);
        }

        public static DateTime ConvertFromSASDateTime(this double value)
        {
            return new DateTime(1960, 1, 1).AddDays(value);
        }
    }
}
