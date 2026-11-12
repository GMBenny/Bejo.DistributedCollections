namespace Bejo.DistributedCollections.Network;

internal class ReplicationManager
{
    private readonly ReplicationSetSettings _replicationSetSettings;
    private readonly List<ChangeTracker> _changeTrackers = new List<ChangeTracker>();

    public ReplicationManager(ReplicationSetSettings replicationSetSettings)
    {
        _replicationSetSettings = replicationSetSettings;
        
        foreach (var nodeSettings in replicationSetSettings.nodes)
        {
            _changeTrackers.Add(new ChangeTracker(nodeSettings));
        }
    }
}