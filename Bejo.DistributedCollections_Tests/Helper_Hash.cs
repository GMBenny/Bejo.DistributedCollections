using FluentAssertions;


namespace Bejo.DistributedCollections_Tests;

public class Helper_Hash
{
    [Fact]
    public void Hashtest_1000000Random()
    {
        var uniqueData = GetRandomUniqueByteArrays(1_000_000, 6); 

        var result = new List<ulong>(uniqueData.Count);
        foreach (var data in uniqueData)
        {
            result.Add(DistributedCollections.Helpers.Helper.Hash(data));
        }
        result.Should().OnlyHaveUniqueItems();

        var test = GetULongList(10);

        List<int> counts = new List<int>(test.Count - 1);
        for (int i = 0; i < test.Count - 1; i++)
        {
            counts.Add(result.Count(p => test[i] < p && p < test[i + 1]));
        }
        counts.Should().AllSatisfy(p => p.Should().BeGreaterThan(50_000)); // None must be more than 5%.
        counts.Should().AllSatisfy(p => p.Should().BeLessThan(200_000)); // None must be more than 20%.
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
            var key = Convert.ToHexString(buffer);
            if (set.Add(key))
            {
                result.Add((byte[])buffer.Clone());
            }
        }
        return result;
    }
    private static List<ulong> GetULongList(int length)
    {
        var r = new List<ulong>(length);

        ulong interval = ulong.MaxValue / (ulong)length;

        for (int i = 0; i <= length; i++)
        {
            r.Add(interval * (ulong)i);
        }
        return r;
    }
}
