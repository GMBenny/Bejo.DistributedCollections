using Bejo.DistributedCollections.Contracts;
using System.Collections.Concurrent;
namespace Bejo.DistributedCollections.Network;

// Sends commands to a specific node and monitors its status
internal class CommandHandler
{
    private readonly NodeSettings _nodeSettings;
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
    private readonly SemaphoreSlim _authenticationLock = new(1, 1);
    private volatile bool _authenticated;

    internal CommandHandler(NodeSettings nodeSettings, int retryCount, TimeSpan timeout, bool compress, string? masterKey, string nodeName, string? nodeKey, CancellationToken cancellationToken)
    {
        _nodeSettings = nodeSettings;
        _protocol = new Protocol(_nodeSettings.Host, _nodeSettings.Port, retryCount, timeout, compress, masterKey, nodeName, nodeKey);

        _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        ForeignNodeStat = new NetworkStat(
            $"{_nodeSettings.Host}:{_nodeSettings.Port}",
            DateTime.UtcNow,
            nodeKey == null ? NetworkStatus.Offline : NetworkStatus.NoNodeKey,
            TimeSpan.Zero);

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
            await Task.Delay(pingInterval, token); // Check every pingInterval

            if (!_authenticated)
            {
                await TryAuthenticateAsync(token);
            }
            else
            {
                ForeignNodeStat = await _protocol.SendPingAsync();
            }
        }
    }

    private async Task EstablishInitialConnectionAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            await TryAuthenticateAsync(cancellationToken);
            if (_authenticated)
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
                var remoteStatus = await _protocol.SendHelloAsync();
                _authenticated = true;
                ForeignNodeStat = ForeignNodeStat with
                {
                    LastStatus = remoteStatus,
                    LastRoundtripUTC = DateTime.UtcNow
                };
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _authenticated = false;
            ForeignNodeStat = ForeignNodeStat with
            {
                LastStatus = ex.Message.Contains("Node key rejected", StringComparison.Ordinal) ? NetworkStatus.NoNodeKey : NetworkStatus.Offline,
                LastRoundtripUTC = DateTime.UtcNow
            };
        }
        finally
        {
            _authenticationLock.Release();
        }
    }

    internal async Task SendMessage(string message)
    {
        await _initialHandshakeTask;
        if (!_authenticated)
            await TryAuthenticateAsync(CancellationToken.None);

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
            await TryAuthenticateAsync(CancellationToken.None);

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
}