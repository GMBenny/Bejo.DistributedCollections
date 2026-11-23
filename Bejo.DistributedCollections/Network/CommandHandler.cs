using Bejo.DistributedCollections.Contracts;
using System.Collections.Concurrent;
namespace Bejo.DistributedCollections.Network;

// Sends commands to a specific node and monitors its status
internal class CommandHandler
{
    private readonly NodeSettings _nodeSettings;
    internal string Address => $"{_nodeSettings.Host}:{_nodeSettings.Port}";
    internal NetworkStat ForeignNodeStat { get; private set; } = new NetworkStat("", DateTime.MinValue, NetworkStatus.Offline, TimeSpan.Zero);

    // Command queue
    private readonly ConcurrentQueue<CommandDTO> _changes = new();

    // Ensure only one command is processed at a time for this node
    private readonly SemaphoreSlim _signal = new(0);

    // Background tasks for processing commands and monitoring node status
    private readonly Task? _processChangesTask;
    private readonly Task? _nodeStatTask;
    private readonly Task _initialHandshakeTask;

    private CancellationTokenSource? _cancellationTokenSource;

    private readonly Protocol _protocol;
    private readonly Action<string, string, int, NetworkStatus>? _peerConnected;
    private readonly SemaphoreSlim _authenticationLock = new(1, 1);
    private volatile bool _authenticated;
    private volatile bool _authenticationRejected;
    private volatile bool _hasSuccessfulContact;
    private string? _remoteNodeName;
    private DateTime _lastSuccessfulContactUtc;

    private static readonly TimeSpan OfflineAfter = TimeSpan.FromSeconds(30);

    internal bool AuthenticationRejected => _authenticationRejected;
    internal string? RemoteNodeName => _remoteNodeName;

    internal CommandHandler(NodeSettings nodeSettings, int retryCount, TimeSpan timeout, bool compress, string? masterKey, string nodeName, string? nodeKey, int listenerPort, Action<string, string, int, NetworkStatus>? peerConnected, CancellationToken cancellationToken)
    {
        _nodeSettings = nodeSettings;
        _peerConnected = peerConnected;
        _protocol = new Protocol(_nodeSettings.Host, _nodeSettings.Port, retryCount, timeout, compress, masterKey, nodeName, nodeKey, listenerPort);

        _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        ForeignNodeStat = new NetworkStat(
            $"{_nodeSettings.Host}:{_nodeSettings.Port}",
            DateTime.UtcNow,
            nodeKey == null ? NetworkStatus.Offline : NetworkStatus.NoNodeKey,
            TimeSpan.Zero);
        _lastSuccessfulContactUtc = DateTime.UtcNow;

        _initialHandshakeTask = EstablishInitialConnectionAsync(_cancellationTokenSource.Token);
        _signal.Release();

        _processChangesTask = ProcessCommandAsync(_cancellationTokenSource.Token);
        _nodeStatTask = MonitorNodeStatusAsync(_nodeSettings.PingInterval, _cancellationTokenSource.Token);
    }

    // Recieves a command and adds it to the queue for processing
    internal void SendCommand(CommandDTO changeDto)
    {
        _changes.Enqueue(changeDto);

        // Tell the queue that there is a change to process
        _signal.Release();
    }

    // Processes commands in the queue and sends them to the node
    private async Task ProcessCommandAsync(CancellationToken cancellationToken)
    {
        await _initialHandshakeTask;

        while (!cancellationToken.IsCancellationRequested)
        {
            // If not authenticated, try to authenticate first before processing commands
            if (_authenticationRejected)
                return;

            if (!_authenticated)
            {
                await TryAuthenticateAsync(cancellationToken);
                if (!_authenticated)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(100), cancellationToken);
                    continue;
                }
            }

            // if there is nothing in the queue, wait for something to arrive
            if (_changes.IsEmpty)
                await _signal.WaitAsync(cancellationToken);

            // if there is something in the queue, take it and send it to the other nodes
            if (_changes.TryPeek(out var command))
            {
                try
                {
                    // Sending command through the protocol to the node
                    var x = await _protocol.SendCommand(command);

                    // TODO: Figure out how to handle the response from the node, if needed as this is asynchronous and we don't want to block the queue.
                    // For now, we just dequeue the command after sending it.

                    _changes.TryDequeue(out var _);
                }
                catch (Exception e)
                {
                    _authenticated = false;
                    ForeignNodeStat = ForeignNodeStat with
                    {
                        LastStatus = NetworkStatus.Offline,
                        LastRoundtripUTC = DateTime.UtcNow
                    };
                    // TODO: Do we try and resend the command? For now, we just log the error and continue processing the next command.
                    // if - throw new InvalidOperationException("Response hash does not match"); - We might resend, but keep counting if it happens again...
                    Console.WriteLine($"Error sending command to node {_nodeSettings.Host}:{_nodeSettings.Port} - {e.Message}");
                }
                                
                
            }
        }
    }

    // Monitors the status of the node by sending periodic ping messages
    private async Task MonitorNodeStatusAsync(TimeSpan pingInterval, CancellationToken token)
    {
        await _initialHandshakeTask;

        while (!token.IsCancellationRequested)
        {
            if (_authenticationRejected)
                return;

            await Task.Delay(pingInterval, token); // Check every pingInterval

            if (!_authenticated)
            {
                await TryAuthenticateAsync(token);
            }
            else
            {
                var ping = await _protocol.SendPingAsync();
                if (ping.LastStatus == NetworkStatus.Offline ||
                    ping.LastStatus == NetworkStatus.UnexpectedResult)
                {
                    _authenticated = false;
                    var now = DateTime.UtcNow;
                    var status = now - _lastSuccessfulContactUtc >= OfflineAfter
                        ? NetworkStatus.Offline
                        : NetworkStatus.Suspect;

                    ForeignNodeStat = ForeignNodeStat with
                    {
                        NodeName = _remoteNodeName,
                        LastRoundtripUTC = now,
                        LastStatus = status,
                        RoundtripTime = ping.RoundtripTime
                    };
                }
                else
                {
                    _hasSuccessfulContact = true;
                    _lastSuccessfulContactUtc = ping.LastRoundtripUTC;
                    ForeignNodeStat = ping with { NodeName = _remoteNodeName };
                }
            }
        }
    }

    private async Task EstablishInitialConnectionAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            await TryAuthenticateAsync(cancellationToken);
            if (_authenticated || _authenticationRejected)
                return;

            await Task.Delay(TimeSpan.FromMilliseconds(100), cancellationToken);
        }
    }

    private async Task TryAuthenticateAsync(CancellationToken cancellationToken)
    {
        await _authenticationLock.WaitAsync(cancellationToken);
        try
        {
            if (!_authenticated)
            {
                // HELLO is the only way to mark the remote node as authenticated.
                var remote = await _protocol.SendHelloAsync();
                _authenticated = true;
                _remoteNodeName = remote.NodeName;
                ForeignNodeStat = ForeignNodeStat with
                {
                    NodeName = remote.NodeName,
                    LastStatus = remote.Status,
                    LastRoundtripUTC = DateTime.UtcNow
                };
                _hasSuccessfulContact = true;
                _lastSuccessfulContactUtc = DateTime.UtcNow;
                _peerConnected?.Invoke(remote.NodeName, _nodeSettings.Host, remote.ListenerPort, remote.Status);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _authenticated = false;
            _authenticationRejected = ex.Message.Contains("Node key rejected", StringComparison.Ordinal);
            ForeignNodeStat = ForeignNodeStat with
            {
                LastStatus = _authenticationRejected
                    ? NetworkStatus.NoNodeKey
                    : GetConnectionFailureStatus(),
                LastRoundtripUTC = DateTime.UtcNow
            };
        }
        finally
        {
            _authenticationLock.Release();
        }
    }

    private NetworkStatus GetConnectionFailureStatus()
    {
        if (!_hasSuccessfulContact)
            return NetworkStatus.Offline;

        return DateTime.UtcNow - _lastSuccessfulContactUtc >= OfflineAfter
            ? NetworkStatus.Offline
            : NetworkStatus.Suspect;
    }

    internal async Task SendMessage(string message)
    {
        await _initialHandshakeTask;
        if (!_authenticated)
        {
            if (_authenticationRejected)
                throw new InvalidOperationException($"Node {_nodeSettings.Host}:{_nodeSettings.Port} rejected this node key.");

            await TryAuthenticateAsync(CancellationToken.None);
        }

        if (!_authenticated)
            throw new InvalidOperationException($"Node {_nodeSettings.Host}:{_nodeSettings.Port} is not authenticated.");

        try
        {
            await _protocol.SendSystemMessage(message);
        }
        catch
        {
            _authenticated = false;
            ForeignNodeStat = ForeignNodeStat with
            {
                LastStatus = NetworkStatus.Offline,
                LastRoundtripUTC = DateTime.UtcNow
            };
            throw;
        }
    }

    internal async Task<SyncResponse> SendSyncRequestAsync(SyncRequest request)
    {
        await _initialHandshakeTask;
        if (!_authenticated)
        {
            if (_authenticationRejected)
                throw new InvalidOperationException($"Node {_nodeSettings.Host}:{_nodeSettings.Port} rejected this node key.");

            await TryAuthenticateAsync(CancellationToken.None);
        }

        if (!_authenticated)
            throw new InvalidOperationException($"Node {_nodeSettings.Host}:{_nodeSettings.Port} is not authenticated.");

        try
        {
            return await _protocol.SendSyncRequestAsync(request);
        }
        catch
        {
            _authenticated = false;
            ForeignNodeStat = ForeignNodeStat with
            {
                LastStatus = NetworkStatus.Offline,
                LastRoundtripUTC = DateTime.UtcNow
            };
            throw;
        }
    }

    internal async Task SendMembershipAsync(MembershipUpdate update)
    {
        await _initialHandshakeTask;
        if (!_authenticated)
        {
            if (_authenticationRejected)
                throw new InvalidOperationException($"Node {_nodeSettings.Host}:{_nodeSettings.Port} rejected this node key.");

            await TryAuthenticateAsync(CancellationToken.None);
        }

        if (!_authenticated)
            throw new InvalidOperationException($"Node {_nodeSettings.Host}:{_nodeSettings.Port} is not authenticated.");

        await _protocol.SendMembershipAsync(update);
    }

    internal void Stop()
    {
        _cancellationTokenSource?.Cancel();
        _protocol.Dispose();
    }
}