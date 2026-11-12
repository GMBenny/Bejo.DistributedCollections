namespace Bejo.DistributedCollections.Network;

internal class ReplicationManager
{
    private readonly List<CommandQueue> _changeTrackers = new List<CommandQueue>();

    public ReplicationManager(ReplicationSetSettings replicationSetSettings)
    {
        foreach (var nodeSettings in replicationSetSettings.nodes)
        {
            _changeTrackers.Add(new CommandQueue(nodeSettings, replicationSetSettings.retryCount, replicationSetSettings.timeout, replicationSetSettings.compressData, replicationSetSettings.passwords?.FirstOrDefault() ));
        }
    }

    internal void SendCommand(string collectionName, ChangeDTO changeDto)
    {
        foreach (var c in _changeTrackers)
        {
            c.SendCommand(collectionName, changeDto);
        }
    }
}   