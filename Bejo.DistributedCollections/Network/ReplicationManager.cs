namespace Bejo.DistributedCollections.Network;

internal class ReplicationManager
{
    private readonly List<CommandQueue> _commandQueues = new List<CommandQueue>();

    public ReplicationManager(ReplicationSetSettings replicationSetSettings, Dictionary<string, IDistributedCollection> distributedCollections)
    {
        var server = new Server(
            replicationSetSettings.listenerPort,
            replicationSetSettings.compress,
            replicationSetSettings.passwords,
            new Protocol(distributedCollections).MessageHandler);
        _ = server.StartAsync();

        foreach (var nodeSettings in replicationSetSettings.nodes)
        {
            _commandQueues.Add(new CommandQueue(nodeSettings, replicationSetSettings.retryCount, replicationSetSettings.timeout, replicationSetSettings.compressData, replicationSetSettings.passwords?.FirstOrDefault(), CancellationToken.None));
        }
    }

    internal void SendCommand(ChangeDTO changeDto)
    {
        foreach (var c in _commandQueues)
        {
            c.SendCommand(changeDto);
        }
    }
}   