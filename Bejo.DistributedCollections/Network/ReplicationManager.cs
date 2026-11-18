using Bejo.DistributedCollections.Contracts;

namespace Bejo.DistributedCollections.Network;

// Handles all nodes in the replication set, as well as receiving commands from other nodes.
internal class ReplicationManager : ICommandSender
{
    private readonly List<CommandHandler> _commandhandlers = new List<CommandHandler>();
    private readonly Server _server;
    private Task? _serverTask;

    internal Server.ServerState ServerState => _serverTask?.IsFaulted == true ? Server.ServerState.Faulted : _server.State;

    internal NetworkStat[] GetNetworkStats()
    {
        return _commandhandlers.Select(q => q.NetworkStat).ToArray();
    }


    public ReplicationManager(ReplicationSetSettings replicationSetSettings, ICommandReceiver receiver)
    {
        // Creates server and the delegate to handle incoming messages.
        _server = new Server(replicationSetSettings.listenerPort, replicationSetSettings.compressData, replicationSetSettings.passwords, new Protocol(receiver).MessageHandler);

        // Start client through CommandHandlers and Protocol
        foreach (var nodeSettings in replicationSetSettings.nodes)
        {
            _commandhandlers.Add(new CommandHandler(nodeSettings, replicationSetSettings.retryCount, replicationSetSettings.timeout, replicationSetSettings.compressData, replicationSetSettings.passwords?.FirstOrDefault(), CancellationToken.None));
        }
    }

    // Start server
    internal void Start()
    {
        _serverTask = _server.StartAsync();
    }

    // Receive command and relay to all nodes in the replication set.
    public void SendCommand(CommandDTO changeDto)
    {
        foreach (var c in _commandhandlers)
        {
            c.SendCommand(changeDto);
        }
    }

    internal async Task SendMessage(string message)
    {
        foreach (var c in _commandhandlers)
        {
            await c.SendMessage(message);
        }
    }
}   