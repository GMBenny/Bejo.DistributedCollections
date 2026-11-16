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
}