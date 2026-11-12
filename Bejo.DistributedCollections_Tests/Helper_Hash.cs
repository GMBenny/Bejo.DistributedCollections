using FluentAssertions;


namespace Bejo.DistributedCollections_Tests;

public class Helper_Hash
{
    [Fact]
    public void Hashtest_1000000Random()
    {
        var uniqueData = GetRandomUniqueByteArrays(1000000, 6); 

        var result = new List<ulong>(uniqueData.Count);
        foreach (var data in uniqueData)
        {
            result.Add(DistributedCollections.Helpers.Helper.Hash(data));
        }
        // Assert that all hashes are unique
        result.Should().OnlyHaveUniqueItems();
    }


    [Fact]
    public void Hashtest_Predefined()
    {
        var uniqueData = new List<byte[]>
        {
            new byte[] { 0 },
            new byte[] { 0,0,0,0,0,0,0,0 },
            new byte[] { 255 },
            new byte[] { 255, 255,255,255,255,255,255,255 }
        };

        var result = new List<ulong>(uniqueData.Count);
        foreach (var data in uniqueData)
        {
            result.Add(DistributedCollections.Helpers.Helper.Hash(data));
        }
        // Assert that all hashes are unique
        result.Should().OnlyHaveUniqueItems();
    }

    private static List<byte[]> GetRandomUniqueByteArrays(int count, int length = 10)
    {
        var set = new HashSet<string>(count);
        var result = new List<byte[]>(count);
        var rng = Random.Shared;
        var buffer = new byte[length];

        while (result.Count < count)
        {
            rng.NextBytes(buffer);
            // Brug en streng som nøgle til HashSet (byte[] har ikke strukturel lighed)
            var key = Convert.ToHexString(buffer);
            if (set.Add(key))
            {
                result.Add((byte[])buffer.Clone());
            }
        }
        return result;
    }
}
