using Bejo.DistributedCollections.Contracts;
using System.Collections.Concurrent;
using System.Text.Json;

namespace Bejo.DistributedCollections.Network;

// Handles all nodes in the replication set, as well as receiving commands from other nodes.
internal class ReplicationManager : ICommandSender
{
    private readonly List<CommandHandler> _commandhandlers = new List<CommandHandler>();
    private readonly ConcurrentDictionary<string, NetworkStat> _incomingNodes = new();
    private readonly Server _server;
    private Task? _serverTask;

    internal Server.ServerState ServerState => _serverTask?.IsFaulted == true ? Server.ServerState.Faulted : _server.State;

    // Get network stats for all nodes in the replication set.
    internal NetworkStat[] GetNetworkStats => _commandhandlers
        .Select(q => q.ForeignNodeStat)
        .Where(stat => stat.LastStatus != NetworkStatus.NoNodeKey)
        .Concat(_incomingNodes.Values)
        .ToArray();

    //internal NetworkStat[] GetOnlineNetworkStats()
    //{
    //    return _commandhandlers
    //        .Select(q => q.ForeignNodeStat)
    //        .Where(stat => stat.LastStatus == NetworkStatus.Online)
    //        .ToArray();
    //}


    internal async Task SynchronizeAsync(ReplicationSet localNode, CancellationToken cancellationToken = default)
    {
        // If there are no other nodes in the replication set, there is nothing to synchronize with.
        if (_commandhandlers.Count == 0)
            return;

        // If no cancellation token is provided, create a timeout cancellation token of 10 seconds.
        using var timeoutCts = cancellationToken == default ? new CancellationTokenSource(TimeSpan.FromSeconds(10)) : null;
        cancellationToken = timeoutCts?.Token ?? cancellationToken;

        // Await until at least one node is online, or the cancellation token is triggered.
        CommandHandler? source;
        do
        {
            cancellationToken.ThrowIfCancellationRequested();
            source = GetOnlineCommandHandler();
            if (source == null)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(100), cancellationToken);
            }
        }
        while (source == null);

        // Request collection metadata from the first online node.
        var collectionsResponse = await source.SendSyncRequestAsync(new SyncRequest("COLLECTIONS", null, string.Empty));

        // Deserialize the collection metadata response.
        var remoteCollections = JsonSerializer.Deserialize<SyncCollectionMetadata[]>(collectionsResponse.JsonPayload)
            ?? throw new InvalidOperationException("Invalid collection metadata response.");

        // Synchronize each collection with the local node.
        foreach (var remoteCollection in remoteCollections)
        {
            // Ensure the local node has a corresponding sync collection.
            localNode.EnsureSyncCollection(remoteCollection);

            // Get the local sync collection node for the remote collection.
            var localCollection = localNode.GetSyncCollectionNode(remoteCollection.Name);


            // Attempt to synchronize the collection up to 3 times.
            var synchronized = false;
            for (var attempt = 0; attempt < 3; attempt++)
            {
                // Get the sync hashes for the local collection.
                var hashes = localCollection.GetSyncHashes;

                // Send the collections hash[] to the remote node and receive the changes response.
                var hashResponse = await source.SendSyncRequestAsync(new SyncRequest("HASHES", remoteCollection.Name, JsonSerializer.Serialize(hashes)));

                // Deserialize the changes response from the remote node.
                var changesResponse = JsonSerializer.Deserialize<SyncChangesResponse>(hashResponse.JsonPayload) ?? throw new InvalidOperationException($"Invalid sync response for collection '{remoteCollection.Name}'.");

                // Apply the changes to the local collection.
                localCollection.ApplySyncChanges(changesResponse.Changes, changesResponse.RemoteHashes);

                // Writes to remote during collection is not synchronized, so we take at least one more loop (attempt > 0)
                // If the collection is synchronized (no changes), break the loop.
                if (attempt > 0 && changesResponse.Changes.Length == 0)
                {
                    synchronized = true;
                    break;
                }
            }

            // If the collection could not be synchronized after 3 attempts, throw an exception.
            if (!synchronized)
                throw new InvalidOperationException($"Collection '{remoteCollection.Name}' changed continuously during synchronization.");
        }

        // Notify the remote node that synchronization is complete.
        await source.SendSyncRequestAsync(new SyncRequest("COMPLETE", null, string.Empty));
    }


    public ReplicationManager(ReplicationSetSettings replicationSetSettings, ICommandReceiver receiver)
    {
        // Creates server and the delegate to handle incoming messages.
        var protocol = new Protocol(
            receiver,
            replicationSetSettings.nodeName,
            replicationSetSettings.nodeKey,
            () => receiver is ReplicationSet set ? set.GetLocalStatus() : ThisStatus.Online,
            RegisterIncomingNode);
        
        _server = new Server(
            replicationSetSettings.listenerPort,
            replicationSetSettings.compressData,
            replicationSetSettings.masterKeys,
            (message, authenticated, remoteAddress) => protocol.AuthenticatedMessageHandlerWithAddress(message, authenticated, remoteAddress));

        // Start client through CommandHandlers and Protocol
        foreach (var nodeSettings in replicationSetSettings.nodes)
        {
            _commandhandlers.Add(new CommandHandler(nodeSettings, replicationSetSettings.retryCount, replicationSetSettings.timeout, replicationSetSettings.compressData, replicationSetSettings.masterKeys?.FirstOrDefault(), replicationSetSettings.nodeName, replicationSetSettings.nodeKey, CancellationToken.None));
        }
    }

    private void RegisterIncomingNode(string nodeName, string address, NetworkStatus status)
    {
        _incomingNodes[nodeName] = new NetworkStat(address, DateTime.UtcNow, status, TimeSpan.Zero);
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

    // Get the first online node, ordered by roundtrip time and address.
    private CommandHandler? GetOnlineCommandHandler()
    {
        return _commandhandlers
            .Where(handler => handler.ForeignNodeStat.LastStatus == NetworkStatus.Online)
            .OrderBy(handler => handler.ForeignNodeStat.RoundtripTime == TimeSpan.Zero ? 1 : 0)
            .ThenBy(handler => handler.ForeignNodeStat.RoundtripTime)
            .ThenBy(handler => handler.ForeignNodeStat.Address, StringComparer.Ordinal)
            .FirstOrDefault();
    }
}