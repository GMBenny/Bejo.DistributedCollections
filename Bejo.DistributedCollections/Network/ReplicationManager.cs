using Bejo.DistributedCollections.Contracts;

namespace Bejo.DistributedCollections.Network;

internal class ReplicationManager : ICommandSender
{
    private readonly List<CommandHandler> _commandQueues = new List<CommandHandler>();
    private readonly Server _server;
    private Task? _serverTask;

    internal Server.ServerState ServerState => _serverTask?.IsFaulted == true ? Server.ServerState.Faulted : _server.State;

    internal NetworkStat[] GetNetworkStats()
    {
        return _commandQueues.Select(q => q.NetworkStat).ToArray();
    }


    public ReplicationManager(ReplicationSetSettings replicationSetSettings, ICommandReceiver receiver)
    {
        _server = new Server(
            replicationSetSettings.listenerPort,
            replicationSetSettings.compress,
            replicationSetSettings.passwords,
            new Protocol(receiver).MessageHandler);

        foreach (var nodeSettings in replicationSetSettings.nodes)
        {
            _commandQueues.Add(new CommandHandler(nodeSettings, replicationSetSettings.retryCount, replicationSetSettings.timeout, replicationSetSettings.compressData, replicationSetSettings.passwords?.FirstOrDefault(), CancellationToken.None));
        }
    }

    internal void Start()
    {
        _serverTask = _server.StartAsync();
    }

    public void SendCommand(CommandDTO changeDto)
    {
        foreach (var c in _commandQueues)
        {
            c.SendCommand(changeDto);
        }
    }
}   