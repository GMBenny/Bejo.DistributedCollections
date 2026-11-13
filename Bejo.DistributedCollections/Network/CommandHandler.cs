using Bejo.DistributedCollections.Contracts;
using System.Collections.Concurrent;

namespace Bejo.DistributedCollections.Network;

internal class CommandHandler
{
    private readonly NodeSettings _nodeSettings;
    internal NetworkStat NetworkStat { get; private set; } = new NetworkStat("", DateTime.MinValue, NetworkStatus.Offline, TimeSpan.Zero);
    
    private readonly ConcurrentQueue<CommandDTO> _changes = new();
    private readonly SemaphoreSlim _signal = new(0);

    // Baggrundsopgave til at behandle ændringer
    private Task? _processChangesTask;
    private Task? _nodeStatTask;

    private CancellationTokenSource? _cancellationTokenSource;

    private readonly Protocol _protocol;

    internal CommandHandler(NodeSettings nodeSettings, int retryCount, TimeSpan timeout, bool compress, string? password, CancellationToken cancellationToken)
    {
        _nodeSettings = nodeSettings;
        _protocol = new Protocol(_nodeSettings.Host, _nodeSettings.Port, retryCount, timeout, compress, password);

//        _address = $"{node.Host}:{node.Port}";

        _protocol = new Protocol(_nodeSettings.Host, _nodeSettings.Port, retryCount, timeout, compress, password);

        _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _signal.Release();

        _processChangesTask = ProcessCommandAsync(_cancellationTokenSource.Token);
        _nodeStatTask = MonitorNodeStatusAsync(_nodeSettings.PingInterval, _cancellationTokenSource.Token);
    }


    internal void SendCommand(CommandDTO changeDto)
    {
        _changes.Enqueue(changeDto);

        // Fortæl køen at der er en ændring at behandle
        _signal.Release();


        // Here you would implement the logic to send the command to the node specified in _nodeSettings.
        // This could involve network communication, serialization, etc.
        // For example:
        // var client = new HttpClient();
        // var response = await client.PostAsJsonAsync($"{_nodeSettings.Address}/api/replicate/{collectionName}", changeDto);
        // response.EnsureSuccessStatusCode();
    }


    private async Task ProcessCommandAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            // hvis der ikke er noget i køen, så venter vi på at der kommer noget
            if (_changes.IsEmpty)
                await _signal.WaitAsync(cancellationToken);

            // Hvis der er noget i køen, så tager vi det og sender det til de andre noder
            if (_changes.TryPeek(out var command))
            {
                try
                {
                    var x = await _protocol.SendCommand(command);

                    _changes.TryDequeue(out var _);
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Error sending command to node {_nodeSettings.Host}:{_nodeSettings.Port} - {e.Message}");
                }
                                
                
            }
        }
    }
    private async Task? MonitorNodeStatusAsync(TimeSpan pingInterval, CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            await Task.Delay(pingInterval, token); // Check every pingInterval

            NetworkStat = await _protocol.SendPingAsync();
        }
    }
}


/*
 
 
 ﻿using System.Collections.Concurrent;
using System.Text.Json;

namespace Bejo.DistributedCollections;

internal class DistributedDictionaryChangeTracker<TValue> : IAsyncDisposable
{
    // Server status
    private NetworkStat _networkStat = new NetworkStat();
    private readonly string _address = string.Empty;

    // Kø til at holde data
    private readonly ConcurrentQueue<ChangeDTO<TValue>> _changes = new();

    // Semaphore til at signalere, når der er nye ændringer at behandle
    private readonly SemaphoreSlim _signal = new(0);

    // Baggrundsopgave til at behandle ændringer
    private Task? _processChangesTask;
    private Task? _nodeStatTask;

    private CancellationTokenSource? _cancellationTokenSource;

    // Settings for denne node
    private readonly DistributedProtocol<TValue> _distributedProtocol;

    // Constructor
    internal DistributedDictionaryChangeTracker(DistributedDictionarySettings.NodeData node, CancellationToken cancellationToken)
    {
        _address = $"{node.Host}:{node.Port}";

        _distributedProtocol = new DistributedProtocol<TValue>(node.Host, node.Port);

        _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _signal.Release();

        _processChangesTask = ProcessChangesAsync(_cancellationTokenSource.Token);
        _nodeStatTask = MonitorNodeStatusAsync(node.PingInterval, _cancellationTokenSource.Token);
    }

    // Asynkron metode til at rydde op og afslutte baggrundsopgaven
    public async ValueTask DisposeAsync()
    {
        _cancellationTokenSource?.Cancel();

        if (_processChangesTask != null)
        {
            try
            {
                await _processChangesTask;
            }
            catch (OperationCanceledException)
            {
                // Expected during shutdown
            }
        }

        if (_nodeStatTask != null)
        {
            try
            {
                await _nodeStatTask;
            }
            catch (OperationCanceledException)
            {
                // Expected during shutdown
            }
        }

        _cancellationTokenSource?.Dispose();
        _signal.Dispose();
        _distributedProtocol.Dispose();
    }

    // Metode til at tilføje en ændring til køen
    internal void AddChangeToQueue(string key, ChangeType action, TValue value, string fromValueHash, string toValueHash) //ChangeDTO<TValue> change)
    {
        // Slette directory hvis action er ClearDictionary
        if (action == ChangeType.ClearDictionary) // Hvis vi skal slette hele dictionary, så skal vi ikke have alle de andre ændringer med, da de ikke længere er relevante.
        {
            var change = new ChangeDTO<TValue> { Action = ChangeType.ClearDictionary };

            _changes.Clear(); // Først slettes alt i kø, da det ikke længere er relevant, da vi sletter hele dictionary
            _changes.Enqueue(change); // Tilføj ClearDictionary ændringen til køen
            _signal.Release(); // Fortæl køen at der er en ændring at behandle
        }
        else
        {
            // Hvis der allerede er en ændring med samme key i køen, så skal vi ignorere den, da vi kun vil have den nyeste ændring for hver key
            if (_changes.Any(p => p.Key == key)) // hvis der allere er objekter med denne key, så skal disse slettes først
            {
                // Find alle ændringer med samme key og marker dem som Ignored eftersom vi ikke kan slette fra en kø
                var changesToRemove = _changes.Where(p => p.Key == key).ToList();
                foreach (var changeToRemove in changesToRemove)
                {
                    changeToRemove.Key = ""; // Sæt key til "" for at markere den som slettet (bliver ignoreret senere)
                    changeToRemove.Value = default!;
                    changeToRemove.Action = ChangeType.Ignored; // Sæt action til Ignored for at markere den som slettet (bliver ignoreret senere)
                    changeToRemove.ToValueHash = ""; // Sæt hash til "" for at markere den som slettet (bliver ignoreret senere)
                    changeToRemove.FromValueHash = ""; // Sæt hash til "" for at markere den som slettet (bliver ignoreret senere)
                }
            }

            // Sæt den nyeste ændring i køen
            var change = new ChangeDTO<TValue> { Key = key, Value = value, Action = action, FromValueHash = fromValueHash, ToValueHash = toValueHash };
            _changes.Enqueue(change);

            // Fortæl køen at der er en ændring at behandle
            _signal.Release();
        }
    }

    // background workers
    private async Task ProcessChangesAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            // hvis der ikke er noget i køen, så venter vi på at der kommer noget
            if (_changes.IsEmpty)
                await _signal.WaitAsync(cancellationToken);

            // Hvis der er noget i køen, så tager vi det og sender det til de andre noder
            if (_changes.TryPeek(out var change))
            {
                // Fordi vi ikke tidligere kunne slette elementer fra køen, så er de blevet markeret som Ignored, og derfor skal vi ignorere dem her
                if (change.Action == ChangeType.Ignored)
                {
                    // Hvis action er Ignored, så skal vi bare tage det af køen og ignorere det
                    _changes.TryDequeue(out var _);
                }
                else if (change.Action == ChangeType.ClearDictionary)
                {
                    var response = await _distributedProtocol.SendChangeAsync(JsonSerializer.Serialize(change));
                }
                else
                {
                    var response = await _distributedProtocol.SendChangeAsync(JsonSerializer.Serialize(change));
                    
                    // hvis alt gik ok, tager vi objektet af køen
                    // TODO: hvis response ikke er true, så skal vi vide om vi skal prøve at gensende - hvis der er blevet gensendt mange gange, hvad så?
                    // TODO: Ved gensende, skal vi vel bare slippe _signal, så vi tager et loop mere?
                    if (response)
                    {
                        _changes.TryDequeue(out var _);
                    }
                }
            }
        }
    }
    private async Task? MonitorNodeStatusAsync(TimeSpan pingInterval, CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            await Task.Delay(pingInterval, token); // Check every pingInterval

            _networkStat = await _distributedProtocol.SendPingAsync();
        }
    }

    internal PublicNetworkStat GetNetworkStatus()
    {
        return new PublicNetworkStat
        {
            Address = _address,
            RoundtripTime = _networkStat.RoundtripTime,
            LastStatus = _networkStat.LastStatus.ToString(),
            LastRoundtripUTC = _networkStat.LastRoundtripUTC
        };
    }
}
 */