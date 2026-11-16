using FluentAssertions;
using Microsoft.VisualStudio.TestPlatform.CommunicationUtilities;

namespace Bejo.DistributedCollections_Tests;

public class DistributedDictionaryTest
{
    [Fact]
    public async Task TryAdd_Sunshine_GetData()
    {
        var rs1 = new DistributedCollections.ReplicationSet(
            new DistributedCollections.ReplicationSetSettings("rs1", 12001, false, null, [new("localhost", 12002, TimeSpan.FromSeconds(2))], 3, TimeSpan.FromSeconds(2)));
        var rs2 = new DistributedCollections.ReplicationSet(
            new DistributedCollections.ReplicationSetSettings("rs2", 12002, false, null, [new("localhost", 12001, TimeSpan.FromSeconds(2))], 3, TimeSpan.FromSeconds(2)));

        await WaitForReplicationSetToStart([rs1, rs2]);

        var dict1 = rs1.GetOrCreateDistributedDictionary<int, string>("d1", HandleConflict);
        var dict2 = rs2.GetOrCreateDistributedDictionary<int, string>("d1", HandleConflict);

        dict1.TryAdd(1, "one");

        await Task.Delay(100);

        dict2[1].Should().Be("one");
    }
    [Fact]
    public async Task Remove_Sunshine_GetData()
    {
        var rs1 = new DistributedCollections.ReplicationSet(
            new DistributedCollections.ReplicationSetSettings("rs1", 12011, false, null, [new("localhost", 12012, TimeSpan.FromSeconds(2))], 3, TimeSpan.FromSeconds(2)));
        var rs2 = new DistributedCollections.ReplicationSet(
            new DistributedCollections.ReplicationSetSettings("rs2", 12012, false, null, [new("localhost", 12011, TimeSpan.FromSeconds(2))], 3, TimeSpan.FromSeconds(2)));

        await WaitForReplicationSetToStart([rs1, rs2]);

        var dict1 = rs1.GetOrCreateDistributedDictionary<int, string>("d2", HandleConflict);
        var dict2 = rs2.GetOrCreateDistributedDictionary<int, string>("d2", HandleConflict);

        dict1.TryAdd(1, "one");

        await Task.Delay(100);

        dict2.Remove(1);

        await Task.Delay(100);

        dict1.ContainsKey(1).Should().BeFalse();
    }
    [Fact]
    public async Task Update_Sunshine_GetData()
    {
        var rs1 = new DistributedCollections.ReplicationSet(
            new DistributedCollections.ReplicationSetSettings("rs1", 12021, false, null, [new("localhost", 12022, TimeSpan.FromSeconds(2))], 3, TimeSpan.FromSeconds(2)));
        var rs2 = new DistributedCollections.ReplicationSet(
            new DistributedCollections.ReplicationSetSettings("rs2", 12022, false, null, [new("localhost", 12021, TimeSpan.FromSeconds(2))], 3, TimeSpan.FromSeconds(2)));

        await WaitForReplicationSetToStart([rs1, rs2]);

        var dict1 = rs1.GetOrCreateDistributedDictionary<int, string>("d1", HandleConflict);
        var dict2 = rs2.GetOrCreateDistributedDictionary<int, string>("d1", HandleConflict);

        dict1.TryAdd(1, "one");

        await Task.Delay(100);

        dict2[1] = "ONE";

        await Task.Delay(100);

        dict1[1].Should().Be("ONE");
    }
    [Fact]
    public async Task Update_MissingKey_Error()
    {
        var rs1 = new DistributedCollections.ReplicationSet(
            new DistributedCollections.ReplicationSetSettings("rs1", 12031, false, null, [new("localhost", 12032, TimeSpan.FromSeconds(2))], 3, TimeSpan.FromSeconds(2)));
        var rs2 = new DistributedCollections.ReplicationSet(
            new DistributedCollections.ReplicationSetSettings("rs2", 12032, false, null, [new("localhost", 12031, TimeSpan.FromSeconds(2))], 3, TimeSpan.FromSeconds(2)));

        await WaitForReplicationSetToStart([rs1, rs2]);

        var dict1 = rs1.GetOrCreateDistributedDictionary<int, string>("d1", HandleConflict);
        var dict2 = rs2.GetOrCreateDistributedDictionary<int, string>("d1", HandleConflict);

        await Task.Delay(100);

        var exception = await Assert.ThrowsAsync<KeyNotFoundException>(async () => { dict1[1].Should().Be("ONE"); });

        exception.Message.Should().Be("The given key was not present in the dictionary.");
    }
    [Fact]
    public async Task HandleConflict_Conflict_SameData()
    {
        var rs1 = new DistributedCollections.ReplicationSet(
            new DistributedCollections.ReplicationSetSettings("rs1", 12041, false, null, [new("localhost", 12042, TimeSpan.FromSeconds(2))], 3, TimeSpan.FromSeconds(2)));
        var rs2 = new DistributedCollections.ReplicationSet(
            new DistributedCollections.ReplicationSetSettings("rs2", 12042, false, null, [new("localhost", 12041, TimeSpan.FromSeconds(2))], 3, TimeSpan.FromSeconds(2)));

        await WaitForReplicationSetToStart([rs1, rs2]);

        var dict1 = rs1.GetOrCreateDistributedDictionary<int, string>("d3", HandleConflict);
        var dict2 = rs2.GetOrCreateDistributedDictionary<int, string>("d3", HandleConflict);

        dict1.TryAdd(1, "one");
        dict2.TryAdd(1, "ONE");

        await Task.Delay(100);

        dict1[1].Should().Be("ONE");
        dict2[1].Should().Be("ONE");
    }

    private string HandleConflict(int key, DistributedCollections.ConflictData<string> a, DistributedCollections.ConflictData<string> b)
    {
        return string.Compare(a.Value, b.Value, StringComparison.Ordinal) < 0 ? a.Value : b.Value;
    }

    private async Task WaitForReplicationSetToStart(DistributedCollections.ReplicationSet[] rs)
    {
        foreach (var r in rs)
        {
            while (r.GetReplicationSetStatus().ThisStatus != DistributedCollections.ThisStatus.Started)
            {
                await Task.Delay(25);
            }
        }        
    }
}