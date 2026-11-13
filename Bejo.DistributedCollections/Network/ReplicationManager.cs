using Bejo.DistributedCollections.Dictionary;

namespace Bejo.DistributedCollections.Network;

internal class ReplicationManager
{
    private readonly List<CommandQueue> _commandQueues = new List<CommandQueue>();
    private readonly Server _server;
    private readonly Task _serverTask;

    public ReplicationManager(ReplicationSetSettings replicationSetSettings, Dictionary<string, CollectionInfo> distributedCollections)
    {
        _server = new Server(
            replicationSetSettings.listenerPort,
            replicationSetSettings.compress,
            replicationSetSettings.passwords,
            new Protocol(distributedCollections, this).MessageHandler);
        _serverTask = _server.StartAsync();

        foreach (var nodeSettings in replicationSetSettings.nodes)
        {
            _commandQueues.Add(new CommandQueue(nodeSettings, replicationSetSettings.retryCount, replicationSetSettings.timeout, replicationSetSettings.compressData, replicationSetSettings.passwords?.FirstOrDefault(), CancellationToken.None));
        }
    }

    internal void SendCommand(CommandDTO changeDto)
    {
        foreach (var c in _commandQueues)
        {
            c.SendCommand(changeDto);
        }
    }
}   