using Bejo.DistributedCollections;

namespace Bejo.DistributedCollections_Tests;

public class ReplicationSetTest
{
    [Fact]
    public void ReplicationSetStatus_ServerNotStarted_Offline()
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

        Assert.Equal(ThisStatus.Started, status.ThisStatus);
        Assert.Equal(NetworkStatus.Offline, Assert.Single(status.NetworkStats).LastStatus);
    }

    [Fact]
    public async Task SendMessage_Sunshine_MessageReceived()
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
    public async Task SendMessage_WithPassword_MessageReceived()
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
    public async Task SendMessage_WithPasswordUpdate_MessageReceived()
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

    private async Task WaitForReplicationSetToStart(ReplicationSet[] rs)
    {
        foreach (var r in rs)
        {
            while (r.GetReplicationSetStatus().ThisStatus != ThisStatus.Started)
            {
                await Task.Delay(25);
            }
        }
    }
}