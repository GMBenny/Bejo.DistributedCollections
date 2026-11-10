namespace Bejo.DistributedCollections.Helpers;


internal static class Helper
{
    internal static ulong Hash(byte[] data)
    {
        ulong hash = 0;

        for (int i = 0; i < data.Length; i++)
        {
            var b = data[i];

            ulong tmp = 0;
            if(i % 8 == 0)
                tmp = (ulong)b << 56;
            else if(i % 8 == 1)
                tmp = (ulong)b << 48;
            else if(i % 8 == 2)
                tmp = (ulong)b << 40;
            else if(i % 8 == 3)
                tmp = (ulong)b << 32;
            else if(i % 8 == 4)
                tmp = (ulong)b << 24;
            else if(i % 8 == 5)
                tmp = (ulong)b << 16;
            else if(i % 8 == 6)
                tmp = (ulong)b << 8;
            else if(i % 8 == 7)
                tmp = (ulong)b;

            hash ^= tmp;

        }
        return hash;        
    }
}