using Bejo.DistributedCollections.Contracts;
using System.Collections.Concurrent;
using System.Text.Json;

namespace Bejo.DistributedCollections.Network;

// Handles all nodes in the replication set, as well as receiving commands from other nodes.
internal class ReplicationManager : ICommandSender
{
    private readonly List<CommandHandler> _commandhandlers = new List<CommandHandler>();
    private readonly object _handlersLock = new();
    private readonly object _membershipLock = new();
    private readonly Dictionary<string, MembershipMember> _members = new();
    private readonly Dictionary<string, MembershipMember> _removedMembers = new();
    private readonly ReplicationSetSettings _settings;
    private readonly string _localNodeName;
    private readonly TimeSpan _defaultPingInterval;
    private long _membershipVersion;
    private readonly ConcurrentDictionary<string, NetworkStat> _incomingNodes = new();
    private readonly Server _server;
    private readonly CancellationTokenSource _serverCancellation = new();
    private Task? _serverTask;

    internal Server.ServerState ServerState => _serverTask?.IsFaulted == true ? Server.ServerState.Faulted : _server.State;

    // Get network stats for all nodes in the replication set.
    internal NetworkStat[] GetNetworkStats
    {
        get
        {
            CommandHandler[] handlers;
            lock (_handlersLock)
                handlers = _commandhandlers.ToArray();

            var stats = new Dictionary<string, NetworkStat>();
            foreach (var handler in handlers)
            {
                if (handler.ForeignNodeStat.LastStatus == NetworkStatus.NoNodeKey)
                    continue;

                var key = handler.RemoteNodeName ?? handler.Address;
                stats[key] = PreferNetworkStat(stats.TryGetValue(key, out var existing) ? existing : null, handler.ForeignNodeStat);
            }

            foreach (var incoming in _incomingNodes)
            {
                var incomingStat = GetIncomingNetworkStat(incoming.Value);
                stats[incoming.Key] = PreferNetworkStat(stats.TryGetValue(incoming.Key, out var existing) ? existing : null, incomingStat);
            }

            return stats.Values.ToArray();
        }
    }

    internal bool HasRejectedNode => _commandhandlers.Any(handler => handler.AuthenticationRejected);

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
        if (GetHandlerSnapshot().Length == 0)
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
                if (GetHandlerSnapshot().All(handler => handler.AuthenticationRejected))
                    throw new InvalidOperationException("All configured nodes rejected this node key.");

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
        _settings = replicationSetSettings;
        _localNodeName = replicationSetSettings.nodeName;
        _defaultPingInterval = replicationSetSettings.nodes.FirstOrDefault()?.PingInterval ?? TimeSpan.FromSeconds(2);
        _members[_localNodeName] = new MembershipMember(_localNodeName, "localhost", replicationSetSettings.listenerPort);

        // Creates server and the delegate to handle incoming messages.
        var protocol = new Protocol(
            receiver,
            replicationSetSettings.nodeName,
            replicationSetSettings.nodeKey,
            () => receiver is ReplicationSet set ? set.GetLocalStatus() : ThisStatus.Online,
            RegisterIncomingNode,
            UpdateIncomingNodeStatus);
        
        _server = new Server(
            replicationSetSettings.listenerPort,
            replicationSetSettings.compressData,
            replicationSetSettings.masterKeys,
            (message, authenticated, remoteAddress) => protocol.AuthenticatedMessageHandlerWithAddress(message, authenticated, remoteAddress));

        // Start client through CommandHandlers and Protocol
        foreach (var nodeSettings in replicationSetSettings.nodes)
            AddCommandHandler(nodeSettings, replicationSetSettings);
    }

    private void AddCommandHandler(NodeSettings nodeSettings, ReplicationSetSettings settings)
    {
        lock (_handlersLock)
        {
            if (_commandhandlers.Any(handler => handler.Address == $"{nodeSettings.Host}:{nodeSettings.Port}"))
                return;

            _commandhandlers.Add(new CommandHandler(
                nodeSettings,
                settings.retryCount,
                settings.timeout,
                settings.compressData,
                settings.masterKeys?.FirstOrDefault(),
                settings.nodeName,
                settings.nodeKey,
                settings.listenerPort,
                RegisterConnectedPeer,
                CancellationToken.None));
        }
    }

    private void RegisterConnectedPeer(string nodeName, string host, int listenerPort, NetworkStatus status)
    {
        if (nodeName == _localNodeName || listenerPort <= 0)
            return;

        UpdateMembership(new MembershipMember(nodeName, NormalizeHost(host), listenerPort));
    }

    private void RegisterIncomingNode(string nodeName, string address, int listenerPort, NetworkStatus status)
    {
        _incomingNodes[nodeName] = new NetworkStat(address, DateTime.UtcNow, status, TimeSpan.Zero)
        {
            NodeName = nodeName
        };
        if (listenerPort > 0)
            UpdateMembership(new MembershipMember(nodeName, NormalizeHost(address), listenerPort));
    }

    internal void HandleMembership(MembershipUpdate update)
    {
        lock (_membershipLock)
        {
            if (update.Version <= _membershipVersion)
                return;

            _membershipVersion = update.Version;
            foreach (var member in update.Members)
            {
                if (member.NodeName != _localNodeName &&
                    !_removedMembers.ContainsKey(member.NodeName))
                    _members[member.NodeName] = member;
            }

            foreach (var removed in update.RemovedMembers ?? Array.Empty<MembershipMember>())
            {
                if (removed.NodeName != _localNodeName)
                {
                    _members.Remove(removed.NodeName);
                    _removedMembers[removed.NodeName] = removed;
                }
            }
        }

        foreach (var member in update.Members)
        {
            if (member.NodeName != _localNodeName)
                AddCommandHandler(new NodeSettings(member.Host, member.Port, _defaultPingInterval), _settings);
        }

        foreach (var removed in update.RemovedMembers ?? Array.Empty<MembershipMember>())
        {
            if (removed.NodeName != _localNodeName)
            {
                _incomingNodes.TryRemove(removed.NodeName, out _);
                RemoveCommandHandler(removed.NodeName);
            }
        }

        BroadcastMembership(update);
    }

    private void UpdateMembership(MembershipMember member)
    {
        MembershipUpdate update;
        lock (_membershipLock)
        {
            if (_members.TryGetValue(member.NodeName, out var existing) && existing == member)
                return;

            _removedMembers.Remove(member.NodeName);
            _members[member.NodeName] = member;
            update = CreateMembershipUpdate(++_membershipVersion);
        }

        AddCommandHandler(new NodeSettings(member.Host, member.Port, _defaultPingInterval), _settings);
        BroadcastMembership(update);
    }

    private MembershipUpdate CreateMembershipUpdate(long version)
    {
        return new MembershipUpdate(
            version,
            _members.Values.ToArray(),
            _removedMembers.Values.ToArray());
    }

    private async Task BroadcastMembershipAsync(MembershipUpdate update)
    {
        CommandHandler[] handlers;
        lock (_handlersLock)
            handlers = _commandhandlers.ToArray();

        await Task.WhenAll(handlers.Select(handler => handler.SendMembershipAsync(update)));
    }

    private void BroadcastMembership(MembershipUpdate update)
    {
        _ = BroadcastMembershipAsync(update);
    }

    private void RemoveCommandHandler(string nodeName)
    {
        CommandHandler[] removed;
        lock (_handlersLock)
        {
            removed = _commandhandlers
                .Where(handler => string.Equals(handler.RemoteNodeName, nodeName, StringComparison.Ordinal))
                .ToArray();

            foreach (var handler in removed)
                _commandhandlers.Remove(handler);
        }

        foreach (var handler in removed)
            handler.Stop();
    }

    internal async Task LeaveAsync()
    {
        MembershipUpdate update;
        lock (_membershipLock)
        {
            var localMember = _members.TryGetValue(_localNodeName, out var member)
                ? member
                : new MembershipMember(_localNodeName, "localhost", _settings.listenerPort);

            _members.Remove(_localNodeName);
            _removedMembers[_localNodeName] = localMember;
            update = CreateMembershipUpdate(++_membershipVersion);
        }

        try
        {
            await BroadcastMembershipAsync(update);
        }
        finally
        {
            _serverCancellation.Cancel();
            _server.Stop();
            CommandHandler[] handlers = GetHandlerSnapshot();
            lock (_handlersLock)
                _commandhandlers.Clear();
            foreach (var handler in handlers)
                handler.Stop();
        }
    }

    private CommandHandler[] GetHandlerSnapshot()
    {
        lock (_handlersLock)
            return _commandhandlers.ToArray();
    }

    private static NetworkStat PreferNetworkStat(NetworkStat? current, NetworkStat candidate)
    {
        if (current == null)
            return candidate;

        static int Rank(NetworkStatus status) => status switch
        {
            NetworkStatus.Online => 3,
            NetworkStatus.SyncingData => 2,
            NetworkStatus.Suspect => 1,
            NetworkStatus.LoadingData => 1,
            _ => 0
        };

        return Rank(candidate.LastStatus) >= Rank(current.LastStatus) ? candidate : current;
    }

    private static readonly TimeSpan IncomingOfflineAfter = TimeSpan.FromSeconds(30);

    private NetworkStat GetIncomingNetworkStat(NetworkStat stat)
    {
        var elapsed = DateTime.UtcNow - stat.LastRoundtripUTC;
        var suspectAfter = TimeSpan.FromTicks(Math.Min(
            IncomingOfflineAfter.Ticks,
            Math.Max(TimeSpan.FromSeconds(5).Ticks, _defaultPingInterval.Ticks * 2)));

        if (elapsed < TimeSpan.Zero || elapsed < IncomingOfflineAfter)
        {
            if (elapsed >= TimeSpan.Zero && elapsed > suspectAfter &&
                stat.LastStatus == NetworkStatus.Online)
                return stat with { LastStatus = NetworkStatus.Suspect };

            return stat;
        }

        return stat with { LastStatus = NetworkStatus.Offline };
    }

    private static string NormalizeHost(string address)
    {
        var host = address;
        var lastColon = address.LastIndexOf(':');
        if (lastColon > 0)
            host = address[..lastColon];

        return host.Trim('[', ']');
    }

    private void UpdateIncomingNodeStatus(string address, NetworkStatus status)
    {
        foreach (var node in _incomingNodes)
        {
            if (node.Value.Address == address)
            {
                _incomingNodes[node.Key] = node.Value with
                {
                    LastStatus = status,
                    LastRoundtripUTC = DateTime.UtcNow
                };
                return;
            }
        }
    }

    // Start server
    internal void Start()
    {
        _serverTask = _server.StartAsync(_serverCancellation.Token);
    }

    // Receive command and relay to all nodes in the replication set.
    public void SendCommand(CommandDTO changeDto)
    {
        foreach (var c in GetHandlerSnapshot())
        {
            c.SendCommand(changeDto);
        }
    }

    internal async Task SendMessage(string message)
    {
        foreach (var c in GetHandlerSnapshot())
        {
            await c.SendMessage(message);
        }
    }

    // Get the first online node, ordered by roundtrip time and address.
    private CommandHandler? GetOnlineCommandHandler()
    {
        return GetHandlerSnapshot()
            .Where(handler =>
                handler.ForeignNodeStat.LastStatus == NetworkStatus.Online ||
                handler.ForeignNodeStat.LastStatus == NetworkStatus.SyncingData)
            .OrderBy(handler => handler.ForeignNodeStat.RoundtripTime == TimeSpan.Zero ? 1 : 0)
            .ThenBy(handler => handler.ForeignNodeStat.RoundtripTime)
            .ThenBy(handler => handler.ForeignNodeStat.Address, StringComparer.Ordinal)
            .FirstOrDefault();
    }
}