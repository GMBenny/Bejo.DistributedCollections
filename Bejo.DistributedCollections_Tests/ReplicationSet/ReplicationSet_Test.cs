using Bejo.DistributedCollections;
using FluentAssertions;

namespace Bejo.DistributedCollections_Tests;

public class ReplicationSetTest
{
    [Fact]
    public void GetReplicationSetStatus_WithUnavailablePeer_SyncingData()
    {
        var settings = new ReplicationSetSettings(
            "NodeA", 13001, false, null,
            [
                new NodeSettings("localhost", 13002, TimeSpan.FromMinutes(1))
            ],
            3, new TimeSpan(0, 0, 2)
        );

        var set = new ReplicationSet(settings);

        var status = set.GetReplicationSetStatus();

        Assert.Equal(ThisStatus.SyncingData, status.ThisStatus);
        Assert.True(status.ServerStarted);
        Assert.Equal(NetworkStatus.Offline, Assert.Single(status.NetworkStats).LastStatus);
    }

    [Fact]
    public async Task SendMessage_ConnectedWithoutPassword_MessageReceived()
    {
        string messageReceived = string.Empty;

        var rs1 = new ReplicationSet(new ReplicationSetSettings("NodeA", 13010, false, null, [new NodeSettings("localhost", 13011, TimeSpan.FromMinutes(1))], 3, new TimeSpan(0, 0, 2)));
        var rs2 = new ReplicationSet(new ReplicationSetSettings("NodeB", 13011, false, null, [new NodeSettings("localhost", 13010, TimeSpan.FromMinutes(1))], 3, new TimeSpan(0, 0, 2)), message => messageReceived = message);

        await WaitForReplicationSetToStart([rs1, rs2]);

        await rs1.SendMessage("Hello from NodeA");

        await Task.Delay(100); // Give some time for the message to be received

        Assert.Equal("Hello from NodeA", messageReceived);
    }

    [Fact]
    public async Task SendMessage_ConnectedWithPassword_MessageReceived()
    {
        string messageReceived = string.Empty;

        var rs1 = new ReplicationSet(new ReplicationSetSettings("NodeA", 13012, false, ["password"], [new NodeSettings("localhost", 13013, TimeSpan.FromMinutes(1))], 3, new TimeSpan(0, 0, 2)));
        var rs2 = new ReplicationSet(new ReplicationSetSettings("NodeB", 13013, false, ["password"], [new NodeSettings("localhost", 13012, TimeSpan.FromMinutes(1))], 3, new TimeSpan(0, 0, 2)), message => messageReceived = message);

        await WaitForReplicationSetToStart([rs1, rs2]);

        await rs1.SendMessage("Hello from NodeA");

        await Task.Delay(100); // Give some time for the message to be received

        Assert.Equal("Hello from NodeA", messageReceived);
    }

    [Fact]
    public async Task SendMessage_MasterKeyUpdated_MessageReceived()
    {
        string messageReceived = string.Empty;

        var rs1 = new ReplicationSet(new ReplicationSetSettings("NodeA", 13014, false, ["password"], [new NodeSettings("localhost", 13015, TimeSpan.FromMinutes(1))], 3, new TimeSpan(0, 0, 2)));
        var rs2 = new ReplicationSet(new ReplicationSetSettings("NodeB", 13015, false, ["password"], [new NodeSettings("localhost", 13014, TimeSpan.FromMinutes(1))], 3, new TimeSpan(0, 0, 2)), message => messageReceived = message);

        await WaitForReplicationSetToStart([rs1, rs2]);

        await rs1.SendMessage("Hello from NodeA");
        await Task.Delay(100); // Give some time for the message to be received
        Assert.Equal("Hello from NodeA", messageReceived);

        // Two master keys to the receiving node - Encrypts with the first master key, decrypts with either of the two master keys
        rs2.UpdateMasterKeys(["password", "newpassword"]);

        await rs1.SendMessage("Hello from NodeA");
        await Task.Delay(100); // Give some time for the message to be received
        Assert.Equal("Hello from NodeA", messageReceived);

        // Two master keys to the sending node - Encrypts with the first master key, decrypts with either of the two master keys
        rs1.UpdateMasterKeys(["password", "newpassword"]);

        await rs1.SendMessage("Hello from NodeA");
        await Task.Delay(100); // Give some time for the message to be received
        Assert.Equal("Hello from NodeA", messageReceived);

        // Remove old master key
        rs2.UpdateMasterKeys(["newpassword"]);

        await rs1.SendMessage("Hello from NodeA");
        await Task.Delay(100); // Give some time for the message to be received
        Assert.Equal("Hello from NodeA", messageReceived);

        // Remove old master key
        rs1.UpdateMasterKeys(["newpassword"]);

        await rs1.SendMessage("Hello from NodeA");
        await Task.Delay(100); // Give some time for the message to be received
        Assert.Equal("Hello from NodeA", messageReceived);
    }

    [Fact]
    public void RestartReplicationSet_WithPersistedDictionary_DictionaryRestored()
    {
        var persistPath = Directory.CreateTempSubdirectory("Bejo.DistributedCollections_").FullName;

        try
        {
            // Create a replication set, add a dictionary entry, and persist it to a file
            var firstReplicationSet = CreateReplicationSet("NodeA", 13020, persistPath);
            var firstDictionary = firstReplicationSet.GetOrCreateDistributedDictionary<int, string>("myDictionary", (key, localValue, distributedValue) => localValue.Value);
            firstDictionary.Add(1, "one");

            // Assert file content
            Assert.True(File.Exists(Path.Combine(persistPath, "DistributedCollections.json"))); // Does the file that is supposed to contain information about the dictionary we just created exist?
            var dictionaryFile = Assert.Single(Directory.GetFiles(persistPath, "DistributedDictionary_*.json")); // Is there a file that could contain the dictionary itself, with a value inside it?
            var distributedCollectionsContent = File.ReadAllText(Path.Combine(persistPath, "DistributedCollections.json"));
            distributedCollectionsContent.Should().Contain("myDictionary"); // Name of collection
            distributedCollectionsContent.Should().Contain("DistributedDictionary"); // Type of collection
            
            var dictionaryContent = File.ReadAllText(dictionaryFile);
            Assert.Contains("\"Key\":1", dictionaryContent); // The key must exist
            Assert.Contains("\"Value\":\"one\"", dictionaryContent); // The value must exist

            // Now, create a new replication set that reads from the same persist path
            var restartedReplicationSet = CreateReplicationSet("NodeB", 13021, persistPath);
            var restoredDictionary = restartedReplicationSet.GetOrCreateDistributedDictionary<int, string>("myDictionary", (key, localValue, distributedValue) => localValue.Value);

            Assert.Equal("one", restoredDictionary[1]);
        }
        finally
        {
            Directory.Delete(persistPath, recursive: true);
        }
    }

    [Fact]
    public async Task NewReplicationSet_WithoutNodeKey_TwoNodeCluster() // Without nodeKey every node are accepted
    {
        // Start first replication set
        var set1 = new ReplicationSet(new ReplicationSetSettings("NodeA", 13031, false, null, [], 3, new TimeSpan(0, 0, 2)));

        // Await it starts
        await WaitForReplicationSetToStart([set1]);

        // Ensure no other nodes in cluster
        set1.GetReplicationSetStatus().NetworkStats.Count().Should().Be(0);

        // Start second replication set, which should join the cluster
        var set2 = new ReplicationSet(new ReplicationSetSettings("NodeB", 13032, false, null, [new NodeSettings("localhost", 13031, TimeSpan.FromMinutes(1))], 3, new TimeSpan(0, 0, 2)));

        // Await both are started
        await WaitForReplicationSetToStart([set1, set2]);
        await Task.Delay(1000); // Give some time for the nodes to recognize each other

        var d1 = set1.GetReplicationSetStatus();
        var d2 = set2.GetReplicationSetStatus();

        // Both nods should now see each other in the cluster
        set1.GetReplicationSetStatus().NetworkStats.Count().Should().Be(1);
        set2.GetReplicationSetStatus().NetworkStats.Count().Should().Be(1);
    }

    [Fact]
    public async Task NewReplicationSet_WithMatchingNodeKey_TwoNodeCluster()
    {
        // Start first replication set
        var set1 = new ReplicationSet(new ReplicationSetSettings("NodeA", 13041, false, null, [], 3, new TimeSpan(0, 0, 2), null, "abc123!!"));

        // Await it starts
        await WaitForReplicationSetToStart([set1]);

        // Ensure no other nodes in cluster
        set1.GetReplicationSetStatus().NetworkStats.Count().Should().Be(0);

        // Start second replication set, which should join the cluster
        var set2 = new ReplicationSet(new ReplicationSetSettings("NodeB", 13042, false, null, [new NodeSettings("localhost", 13041, TimeSpan.FromMinutes(1))], 3, new TimeSpan(0, 0, 2), null, "abc123!!"));

        // Await both are started
        await WaitForReplicationSetToStart([set1, set2]);
        await Task.Delay(1000); // Give the incoming handshake time to update NodeA

        // Both nods should now see each other in the cluster
        set1.GetReplicationSetStatus().NetworkStats.Count().Should().Be(1);
        set2.GetReplicationSetStatus().NetworkStats.Count().Should().Be(1);
    }

    [Fact]
    public async Task NewReplicationSet_WithMismatchingNodeKey_OneNodeCluster()
    {
        // Start first replication set
        var set1 = new ReplicationSet(new ReplicationSetSettings("NodeA", 13051, false, null, [], 3, new TimeSpan(0, 0, 2), null, "abc123!!"));

        // Await it starts
        await WaitForReplicationSetToStart([set1]);

        // Ensure no other nodes in cluster
        set1.GetReplicationSetStatus().NetworkStats.Count().Should().Be(0);

        // Start second replication set, which should join the cluster
        var set2 = new ReplicationSet(new ReplicationSetSettings("NodeB", 13052, false, null, [new NodeSettings("localhost", 13051, TimeSpan.FromMinutes(1))], 3, new TimeSpan(0, 0, 2), null, "wrongcode"));

        // Await both are started
        await WaitForReplicationSetToStart([set1, set2]);

        // Both nods should now see each other in the cluster
        set1.GetReplicationSetStatus().NetworkStats.Count().Should().Be(0);
        set2.GetReplicationSetStatus().NetworkStats.Count().Should().Be(0);
    }

    [Fact]
    public async Task LeaveAsync_RemovesNodeFromCluster()
    {
        var set1 = new ReplicationSet(new ReplicationSetSettings("NodeA", 13071, false, null,
            [new NodeSettings("localhost", 13072, TimeSpan.FromMinutes(1))], 3, TimeSpan.FromSeconds(2)));
        var set2 = new ReplicationSet(new ReplicationSetSettings("NodeB", 13072, false, null,
            [new NodeSettings("localhost", 13071, TimeSpan.FromMinutes(1))], 3, TimeSpan.FromSeconds(2)));

        await WaitForReplicationSetToStart([set1, set2]);

        var set3 = new ReplicationSet(new ReplicationSetSettings("NodeC", 13073, false, null,
            [new NodeSettings("localhost", 13071, TimeSpan.FromMinutes(1))], 3, TimeSpan.FromSeconds(2)));

        await WaitForNetworkCount(set1, 2);
        await WaitForNetworkCount(set2, 2);
        await WaitForNetworkCount(set3, 2);

        await set3.LeaveAsync();

        await WaitForNetworkCount(set1, 1);
        await WaitForNetworkCount(set2, 1);
        set3.GetReplicationSetStatus().ThisStatus.Should().Be(ThisStatus.Stopped);
        set3.GetReplicationSetStatus().ServerStarted.Should().BeFalse();
    }

    [Fact]
    public async Task NewReplicationSet_WithSourceData_DataSynchronized()
    {
        // Start first replication set
        var set1 = new ReplicationSet(new ReplicationSetSettings("NodeA", 13061, false, null, [], 3, new TimeSpan(0, 0, 2)));

        // Await it starts
        await WaitForReplicationSetToStart([set1]);

        // Create a distributed dictionary and add an entry
        var d1 = set1.GetOrCreateDistributedDictionary<int, string>("myDictionary", (key, localValue, distributedValue) => localValue.Value);
        d1.TryAdd(1, "one");

        // -- Ensure no other nodes in cluster --
        set1.GetReplicationSetStatus().NetworkStats.Count().Should().Be(0);

        // Start second replication set, which should join the cluster
        var set2 = new ReplicationSet(new ReplicationSetSettings("NodeB", 13062, false, null, [new NodeSettings("localhost", 13061, TimeSpan.FromMinutes(1))], 3, new TimeSpan(0, 0, 2)));

        // Await both are started : set2 outh to be cycleing through Offline, LoadingData, Syncing, Online and then it's ready
        await WaitForReplicationSetToStart([set1, set2]);
        await WaitForReplicationSetToBeOnline(set2);

        var d2 = set2.GetOrCreateDistributedDictionary<int, string>("myDictionary", (key, localValue, distributedValue) => localValue.Value);
        d2[1].Should().Be("one");
    }

    private static ReplicationSet CreateReplicationSet(string nodeName, int port, string persistPath)
    {
        return new ReplicationSet(new ReplicationSetSettings(nodeName,port,false,null,[],3,TimeSpan.FromSeconds(2),persistPath));
    }

    private async Task WaitForReplicationSetToStart(ReplicationSet[] rs)
    {
        foreach (var r in rs)
        {
            while (!r.GetReplicationSetStatus().ServerStarted)
            {
                await Task.Delay(25);
            }
        }
    }

    private async Task WaitForReplicationSetToBeOnline(ReplicationSet rs)
    {
        while (rs.GetReplicationSetStatus().ThisStatus != ThisStatus.Online)
        {
            rs.GetReplicationSetStatus().ThisStatus.Should().NotBe(ThisStatus.Faulted);
            await Task.Delay(25);
        }
    }

    private async Task WaitForNetworkCount(ReplicationSet rs, int expectedCount)
    {
        for (var attempt = 0; attempt < 100; attempt++)
        {
            if (rs.GetReplicationSetStatus().NetworkStats.Count() == expectedCount)
                return;

            await Task.Delay(50);
        }

        rs.GetReplicationSetStatus().NetworkStats.Count().Should().Be(expectedCount);
    }
}