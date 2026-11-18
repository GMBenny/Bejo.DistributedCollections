using Bejo.DistributedCollections.Contracts;
using System.Collections.Concurrent;

namespace Bejo.DistributedCollections.Network;

// Sends commands to a specific node and monitors its status
internal class CommandHandler
{
    private readonly NodeSettings _nodeSettings;
    internal NetworkStat NetworkStat { get; private set; } = new NetworkStat("", DateTime.MinValue, NetworkStatus.Offline, TimeSpan.Zero);

    // Command queue
    private readonly ConcurrentQueue<CommandDTO> _changes = new();

    // Ensure only one command is processed at a time for this node
    private readonly SemaphoreSlim _signal = new(0);

    // Background tasks for processing commands and monitoring node status
    private readonly Task? _processChangesTask;
    private readonly Task? _nodeStatTask;

    private CancellationTokenSource? _cancellationTokenSource;

    private readonly Protocol _protocol;

    internal CommandHandler(NodeSettings nodeSettings, int retryCount, TimeSpan timeout, bool compress, string? password, CancellationToken cancellationToken)
    {
        _nodeSettings = nodeSettings;
        _protocol = new Protocol(_nodeSettings.Host, _nodeSettings.Port, retryCount, timeout, compress, password);

        _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
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
        while (!cancellationToken.IsCancellationRequested)
        {
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
        while (!token.IsCancellationRequested)
        {
            await Task.Delay(pingInterval, token); // Check every pingInterval

            NetworkStat = await _protocol.SendPingAsync();
        }
    }

    internal async Task SendMessage(string message)
    {
        await _protocol.SendSystemMessage(message);
    }
}