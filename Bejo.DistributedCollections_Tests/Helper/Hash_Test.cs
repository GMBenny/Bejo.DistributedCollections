using FluentAssertions;

namespace Bejo.DistributedCollections_Tests;

public class HashTest
{
    [Fact]
    public void Hash_ThreeMillionUniqueInputs_AllHashesUniqueAndEvenlyDistributed()
    {
        var uniqueData = GetRandomUniqueByteArrays(3_000_000, 6); 

        var result = new List<ulong>(uniqueData.Count);
        foreach (var data in uniqueData)
        {
            result.Add(DistributedCollections.Helpers.Helper.Hash(data));
        }
        result.Should().OnlyHaveUniqueItems();

        var test = GetULongList(40);

        List<int> counts = new List<int>(test.Count - 1);
        for (int i = 0; i < test.Count - 1; i++)
        {
            counts.Add(result.Count(p => test[i] < p && p < test[i + 1]));
        }

        result.Should().HaveCount(3_000_000);
        counts.Sum().Should().Be(3_000_000);
        counts.Should().AllSatisfy(p => p.Should().BeGreaterThan(70_000)); // 3000000/40 = 75_000, so none must be less than 70_000 (-6,7%)
        counts.Should().AllSatisfy(p => p.Should().BeLessThan(80_000)); // 3000000/40 = 75_000, so none must be more than 80_000 (+6,7%)
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
