using Bejo.DistributedCollections;
using Bejo.DistributedCollections.Contracts;
using Bejo.DistributedCollections.Dictionary;
using Bejo.DistributedCollections.Helpers;
using FluentAssertions;
using System.Text.Json;

namespace Bejo.DistributedCollections_Tests;

public class SynchronizationTest
{
    [Fact]
    public async Task InitialSync_WithSourceCollection_TargetOnlineAndDataCopied()
    {
        var source = new ReplicationSet(
            new ReplicationSetSettings("SyncSource", 14001, false, null, [], 3, TimeSpan.FromSeconds(2)));
        var sourceDictionary = source.GetOrCreateDistributedDictionary<int, string>(
            "syncDictionary",
            (_, local, _) => local.Value);
        sourceDictionary.Add(1, "one");

        var target = new ReplicationSet(
            new ReplicationSetSettings(
                "SyncTarget",
                14002,
                false,
                null,
                [new NodeSettings("localhost", 14001, TimeSpan.FromSeconds(1))],
                3,
                TimeSpan.FromSeconds(2)));

        await WaitForStatusAsync(target, ThisStatus.Online);

        target.GetLocalStatusHistory().Should().ContainInOrder(
            ThisStatus.Starting,
            ThisStatus.LoadingData,
            ThisStatus.SyncingData,
            ThisStatus.Online);
        target.GetAllCollections().Should().Contain(item => item.Name == "syncDictionary");

        var targetDictionary = target.GetOrCreateDistributedDictionary<int, string>(
            "syncDictionary",
            (_, local, _) => local.Value);
        targetDictionary[1].Should().Be("one");
        sourceDictionary[1].Should().Be("one");
    }

    [Fact]
    public void ApplySyncChanges_WithAddUpdateAndRemove_NoReplicationAndPersisted()
    {
        var persistPath = Directory.CreateTempSubdirectory("Bejo.DistributedCollections_sync_").FullName;
        try
        {
            var sender = new CountingCommandSender();
            var node = new DistributedDictionaryNode<int, string>(
                "syncDictionary",
                persistPath,
                sender,
                (_, local, _) => local.Value);

            var add = new LocalStorage<int, string>(1, "one");
            var addHash = Helper.Hash(add.JsonString);
            node.ApplySyncChanges(
                [new SyncChange(JsonSerializer.Serialize(1), add.JsonString, ChangeType.AddUpdated, addHash)],
                [new SyncHash(Helper.Hash(JsonSerializer.Serialize(1)), addHash)]);

            sender.Count.Should().Be(0);
            node.TryGetValue(1, out var value).Should().BeTrue();
            value.Should().Be("one");

            var update = new LocalStorage<int, string>(1, "updated");
            var updateHash = Helper.Hash(update.JsonString);
            node.ApplySyncChanges(
                [new SyncChange(JsonSerializer.Serialize(1), update.JsonString, ChangeType.AddUpdated, updateHash)],
                [new SyncHash(Helper.Hash(JsonSerializer.Serialize(1)), updateHash)]);

            node.TryGetValue(1, out value).Should().BeTrue();
            value.Should().Be("updated");

            node.ApplySyncChanges([], []);
            node.TryGetValue(1, out _).Should().BeFalse();
            File.ReadAllText(Path.Combine(
                persistPath,
                $"DistributedDictionary_{Helper.Hash("syncDictionary")}.json")).Should().Be("{}");
        }
        finally
        {
            Directory.Delete(persistPath, recursive: true);
        }
    }

    private static async Task WaitForStatusAsync(ReplicationSet replicationSet, ThisStatus expected)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (replicationSet.GetReplicationSetStatus().ThisStatus != expected)
        {
            replicationSet.GetReplicationSetStatus().ThisStatus.Should().NotBe(ThisStatus.Faulted);
            if (DateTime.UtcNow >= deadline)
                throw new TimeoutException($"Node did not reach status {expected}.");

            await Task.Delay(25);
        }
    }

    private sealed class CountingCommandSender : ICommandSender
    {
        public int Count { get; private set; }

        public void SendCommand(CommandDTO command)
        {
            Count++;
        }
    }
}
