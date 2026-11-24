using Bejo.DistributedCollections.Dictionary;
using Bejo.DistributedCollections.Contracts;
using Bejo.DistributedCollections.Network;
using System.Reflection;
using System.Text.Json;

namespace Bejo.DistributedCollections;


// Manages a set of distributed collections and handles replication commands
public class ReplicationSet : ICommandReceiver, ISyncReceiver, IMembershipReceiver
{
    // Local storage for all distributed collections, keyed by collection name
    // TODO: Disse to er begge CollectionName som key, men forskellig indhold. De bør slåes sammen
    private readonly Dictionary<string, CollectionInfo> _distributedCollections = new();
    //private readonly Dictionary<string, string> _recreateCollectionMetadata = new();

    // ReplicationSet settings
    private ReplicationSetSettings _replicationSetSettings;
    private readonly object _statusLock = new();
    private ThisStatus _thisStatus = ThisStatus.Starting;
    private readonly List<ThisStatus> _statusHistory = new() { ThisStatus.Starting };
    private readonly Task _readyTask;

    // Manages server and client communication for replication
    private readonly ReplicationManager _replicationManager;

    // Low-coupling interface for sending commands to other nodes in the replication set
    private readonly ICommandSender _commandSender;

    private readonly Action<string>? _messageHandler;


    // Create a new ReplicationSet with the specified settings, initializing the replication manager and command sender
    public ReplicationSet(ReplicationSetSettings replicationSetSettings, Action<string>? messageHandler = null)
    {
        // Validate that replicationSetSettings is not null
        ArgumentNullException.ThrowIfNull(replicationSetSettings);

        // Validate that the persist path exists if specified
        if (replicationSetSettings.persistPath != null && !Directory.Exists(replicationSetSettings.persistPath))
            throw new ArgumentException("Persist path must exist or be null.", nameof(replicationSetSettings.persistPath));

        _replicationSetSettings = replicationSetSettings;
        _replicationManager = new ReplicationManager(replicationSetSettings, this);
        _commandSender = _replicationManager;
        _messageHandler = messageHandler;

        // Start the replication manager/server to handle incoming and outgoing replication commands
        _replicationManager.Start();

        SetLocalStatus(ThisStatus.LoadingData);

        // Load existing collection metadata from the persist path if specified, and recreate collections from file
        if (replicationSetSettings.persistPath != null)
        {
            var persistedMetadata = Helpers.Helper.LoadFromFile("DistributedCollections", replicationSetSettings.persistPath, new Dictionary<string, string>());

            foreach (var collectionMetadata in persistedMetadata)
            {
                var dictionaryTypeInfo = JsonSerializer.Deserialize<DictionaryTypeInfo>(collectionMetadata.Value);
                if(dictionaryTypeInfo?.CollectionType == "DistributedDictionary")
                    ValidateAndCreateDistributedDictionary(collectionMetadata.Key, dictionaryTypeInfo, collectionMetadata.Value);
            }

        }

        SetLocalStatus(ThisStatus.SyncingData);

        // If there are no other nodes in the replication set
        if (replicationSetSettings.nodes.Length == 0) // set the local status to Online;
        {
            SetLocalStatus(ThisStatus.Online);
            _readyTask = Task.CompletedTask;
        }
        else
            _readyTask = CompleteInitialSynchronizationAsync(); // otherwise, start the initial synchronization process
    }
    private async Task CompleteInitialSynchronizationAsync()
    {
        try
        {
            await _replicationManager.SynchronizeAsync(this);
            SetLocalStatus(ThisStatus.Online);
        }
        catch (Exception ex)
        {
            _messageHandler?.Invoke($"Initial synchronization failed: {ex.Message}");
            SetLocalStatus(_replicationManager.HasRejectedNode ? ThisStatus.Rejected : ThisStatus.Faulted);
        }
    }

    public void UpdateMasterKeys(string[]? masterKeys)
    {
        _replicationSetSettings = _replicationSetSettings with { masterKeys = masterKeys };
    }

    #region Local information
    public (string Name, string CollectionType)[] GetAllCollections()
    {
        lock (_collectionsLock)
        {
            return _distributedCollections.Select(p => (p.Key, p.Value.CollectionType)).ToArray();
        }
    }
    #endregion

    #region Network stuff
    public ReplicationSetStatus GetReplicationSetStatus()
    {
        if (_replicationManager?.ServerState == Server.ServerState.Faulted)
            SetLocalStatus(ThisStatus.Faulted);

        ThisStatus localStatus;
        lock (_statusLock)
        {
            localStatus = _thisStatus;
        }

        return new ReplicationSetStatus(_replicationSetSettings.nodeName, localStatus, _replicationManager?.GetNetworkStats ?? Array.Empty<NetworkStat>(), _replicationManager?.ServerState == Server.ServerState.Started);
    }

    public async Task WaitUntilReadyAsync(CancellationToken cancellationToken = default)
    {
        await _readyTask.WaitAsync(cancellationToken);

        var status = GetLocalStatus();
        if (status != ThisStatus.Online)
            throw new InvalidOperationException($"Replication set '{_replicationSetSettings.nodeName}' failed to become ready. Current status: {status}.");
    }

    internal void SetLocalStatus(ThisStatus status)
    {
        lock (_statusLock)
        {
            if (_thisStatus != status)
            {
                _thisStatus = status;
                _statusHistory.Add(status);
            }
        }
    }

    internal ThisStatus GetLocalStatus()
    {
        lock (_statusLock)
        {
            return _thisStatus;
        }
    }

    internal ThisStatus[] GetLocalStatusHistory()
    {
        lock (_statusLock)
        {
            return _statusHistory.ToArray();
        }
    }

    // Create and ensure local collection exists
    internal void EnsureSyncCollection(SyncCollectionMetadata metadata)
    {
        lock (_collectionsLock)
        {
            // Get collection metadata from name and ensure the local node has same collection type
            if (_distributedCollections.TryGetValue(metadata.Name, out var existing))
            {
                if (!string.Equals(existing.CollectionType, metadata.CollectionType, StringComparison.Ordinal))
                    throw new InvalidOperationException($"Collection '{metadata.Name}' has a different type.");

                return;
            }

            // Create the collection from metadata
            var dictionaryTypeInfo = JsonSerializer.Deserialize<DictionaryTypeInfo>(metadata.Metadata)
                ?? throw new InvalidOperationException($"Invalid metadata for collection '{metadata.Name}'.");

            // Validate and create the distributed dictionary
            ValidateAndCreateDistributedDictionary(metadata.Name, dictionaryTypeInfo, metadata.Metadata);
            _distributedCollections[metadata.Name] = _distributedCollections[metadata.Name] with { RecreateCollectionMetadata = metadata.Metadata };
            
            // Persist the collection metadata if a persist path is specified
            if (_replicationSetSettings.persistPath != null)
                SaveCollectionMetadata();
        }
    }

    internal IDistributedCollectionNode GetSyncCollectionNode(string collectionName)
    {
        lock (_collectionsLock)
        {
            if (_distributedCollections.TryGetValue(collectionName, out var collection))
                return collection.Node;
        }

        throw new InvalidOperationException($"Collection '{collectionName}' does not exist.");
    }




    // Send a message to all nodes in the replication set
    public async Task SendMessage(string message)
    {
        await _replicationManager.SendMessage(message);
    }

    public async Task LeaveAsync()
    {
        SetLocalStatus(ThisStatus.Leaving);
        try
        {
            await _replicationManager.LeaveAsync();
        }
        finally
        {
            SetLocalStatus(ThisStatus.Stopped);
        }
    }

    void IMembershipReceiver.HandleMembership(MembershipUpdate update)
    {
        _replicationManager.HandleMembership(update);
    }
    #endregion

    #region Distributed Dictionary
    // Delegate for handling conflicts in distributed dictionaries
    public delegate TValue HandleConflict<TKey, TValue>(TKey key, ConflictData<TValue> localValue, ConflictData<TValue> distributedValue);

    // Creates or retrieves a distributed dictionary with the specified name and conflict handler
    public DistributedDictionary<TKey, TValue> GetOrCreateDistributedDictionary<TKey, TValue>(string name, HandleConflict<TKey, TValue> handleConflict) where TKey : notnull
    {
        ValidateKeyType(typeof(TKey));
        ArgumentNullException.ThrowIfNull(handleConflict);
        ArgumentNullException.ThrowIfNullOrWhiteSpace(name);

        lock (_collectionsLock)
        {
            if (_distributedCollections.TryGetValue(name, out var existing))
            {
                if (existing.DistributedCollection is IDictionary<TKey, TValue> dictionary &&
                    existing.Node is DistributedDictionaryNode<TKey, TValue> node)
                {
                    node.SetConflictHandler(handleConflict);
                    return (DistributedDictionary<TKey, TValue>)dictionary;
                }

                throw new InvalidOperationException($"The collection with name '{name}' is not a distributed dictionary");
            }

            var collectionMetadata = JsonSerializer.Serialize(new DictionaryTypeInfo("DistributedDictionary", typeof(TKey).AssemblyQualifiedName!, typeof(TValue).AssemblyQualifiedName!));

            var collection = CreateDictionary<TKey, TValue>(name, handleConflict, collectionMetadata);
            _distributedCollections.Add(name, collection);
            
            // Update with metadata
            _distributedCollections[name] = _distributedCollections[name] with { RecreateCollectionMetadata = collectionMetadata };
            if (_replicationSetSettings.persistPath != null)
                SaveCollectionMetadata();

            _commandSender.SendCommand(new CommandDTO(name, collectionMetadata, ChangeType.CreateCollection, 0, 0));

            return (DistributedDictionary<TKey, TValue>)collection.DistributedCollection;
        }
    }

    // Current accepted key types for distributed dictionaries - If changed, test that remote nodes can still create collections with the same key type.
    private static void ValidateKeyType(Type keyType)
    {
        if (!keyType.IsPrimitive && !keyType.IsEnum && 
            keyType != typeof(string) &&
            keyType != typeof(Guid) && 
            keyType != typeof(decimal) && 
            keyType != typeof(DateTime) &&
            keyType != typeof(DateTimeOffset) && 
            keyType != typeof(TimeSpan) &&
            keyType != typeof(DateOnly) && 
            keyType != typeof(TimeOnly))
        {
            throw new NotSupportedException($"Key type '{keyType}' is not supported. Use a non-nullable primitive, enum, string, Guid, or simple date/time type.");
        }
    }

    private void ValidateAndCreateDistributedDictionary(string collectionName, DictionaryTypeInfo dictionaryTypeInfo, string recreateCollectionMetadata)
    {
        // Validate CollectionType is legal and supported
        if (dictionaryTypeInfo.CollectionType != "DistributedDictionary")
            throw new InvalidOperationException($"Unsupported collection type: {dictionaryTypeInfo.CollectionType}");

        // Get and validate types
        var keyType = Type.GetType(dictionaryTypeInfo.KeyType, throwOnError: true)!;
        var valueType = Type.GetType(dictionaryTypeInfo.ValueType, throwOnError: true)!;
        ValidateKeyType(keyType);

        // Check if the collection already exists and has the same type
        if (_distributedCollections.TryGetValue(collectionName, out var existing))
        {
            var dictionaryType = typeof(IDictionary<,>).MakeGenericType(keyType, valueType);

            // Validate type of found collection
            if (!dictionaryType.IsInstanceOfType(existing.DistributedCollection))
                throw new InvalidOperationException($"Collection '{collectionName}' has a different type.");
            
            return;
        }

        // Create the collection from metadata
        var factory = typeof(ReplicationSet).GetMethod(nameof(CreateRemoteDictionary), BindingFlags.Instance | BindingFlags.NonPublic)!.MakeGenericMethod(keyType, valueType);
        var collection = (CollectionInfo)factory.Invoke(this, new object[] { collectionName, recreateCollectionMetadata })!;
        _distributedCollections.Add(collectionName, collection);

        _distributedCollections[collectionName].Node.LoadDataFromFile();
    }

    private CollectionInfo CreateDictionary<TKey, TValue>(string name, HandleConflict<TKey, TValue> handleConflict, string recreateCollectionMetadata) where TKey : notnull
    {
        var node = new DistributedDictionaryNode<TKey, TValue>(name, _replicationSetSettings.persistPath, _commandSender, handleConflict, () => GetLocalStatus() == ThisStatus.Online);
        var dictionary = new DistributedDictionary<TKey, TValue>(node);
        return new CollectionInfo("DistributedDictionary", dictionary, node, recreateCollectionMetadata);
    }

    private CollectionInfo CreateRemoteDictionary<TKey, TValue>(string name, string recreateCollectionMetadata) where TKey : notnull
    {
        return CreateDictionary<TKey, TValue>(name, (key, localValue, distributedValue) => localValue.Value, recreateCollectionMetadata);
    }

    private void SaveCollectionMetadata()
    {
        var metadata = _distributedCollections.ToDictionary(
            collection => collection.Key,
            collection => collection.Value.RecreateCollectionMetadata);

        Helpers.Helper.SaveToFile("DistributedCollections", _replicationSetSettings.persistPath!, metadata);
    }

    #endregion

    #region ICommandReceiver Implementation
    private readonly object _collectionsLock = new();

    // Handles incoming replication commands, creating collections or delegating to the appropriate collection node by node name
    void ICommandReceiver.HandleCommand(CommandDTO command)
    {
        lock (_collectionsLock)
        {
            if (command.Action == ChangeType.CreateCollection)
            {
                var dictionaryTypeInfo = JsonSerializer.Deserialize<DictionaryTypeInfo>(command.JsonObject);
                if(dictionaryTypeInfo != null)
                    ValidateAndCreateDistributedDictionary(command.CollectionName, dictionaryTypeInfo, command.JsonObject);
            }
            else if (_distributedCollections.TryGetValue(command.CollectionName, out var collection))
            {
                collection.Node.CommandCallback(command);
            }
            else if(command.Action == ChangeType.Message)
            {
                // Handle system message
                _messageHandler?.Invoke(command.JsonObject);
            }
            else
            {
                throw new InvalidOperationException($"Collection '{command.CollectionName}' does not exist.");
            }
        }
    }

    SyncResponse ISyncReceiver.HandleSyncRequest(SyncRequest request)
    {
        if (GetLocalStatus() != ThisStatus.Online && GetLocalStatus() != ThisStatus.SyncingData)
            throw new InvalidOperationException("This node is not ready to provide synchronization data.");

        return request.Action switch
        {
            "COLLECTIONS" => new SyncResponse(
                "COLLECTIONS",
                JsonSerializer.Serialize(_distributedCollections.Select(kv =>
                {
                    var typeInfo = JsonSerializer.Deserialize<DictionaryTypeInfo>(kv.Value.RecreateCollectionMetadata)
                        ?? throw new InvalidOperationException($"Invalid metadata for collection '{kv.Key}'.");
                    return new SyncCollectionMetadata(kv.Key, typeInfo.CollectionType, kv.Value.RecreateCollectionMetadata);
                }).ToArray())),

            "HASHES" => CreateSyncChangesResponse(request),

            "COMPLETE" => new SyncResponse("COMPLETE", string.Empty),

            _ => throw new InvalidOperationException($"Unsupported sync action '{request.Action}'.")
        };
    }

    // Incoming sync request for hashes, return the changes and remote hashes for the specified collection
    private SyncResponse CreateSyncChangesResponse(SyncRequest request)
    {
        // Validate that a collection name is provided for hash synchronization
        if (string.IsNullOrWhiteSpace(request.CollectionName))
            throw new InvalidOperationException("A collection name is required for hash synchronization.");

        // Deserialize the requesting hashes from the request payload
        SyncHash[] requestingHashes = JsonSerializer.Deserialize<SyncHash[]>(request.JsonPayload)
            ?? throw new InvalidOperationException("Invalid collection hashes.");

        lock (_collectionsLock)
        {
            // Ensure the local node has a corresponding sync collection
            if (!_distributedCollections.TryGetValue(request.CollectionName, out var collection))
                throw new InvalidOperationException($"Collection '{request.CollectionName}' does not exist.");

            // Group the requesting hashes by key hash for efficient lookup
            var requestingByKey = requestingHashes.GroupBy(hash => hash.KeyHash).ToDictionary(group => group.Key, group => group.First().ValueHash);

            // Get the local sync entries for the collection and determine which entries have changed compared to the requesting hashes
            var remoteEntries = collection.Node.GetSyncEntries;

            // Determine which entries have changed compared to the requesting hashes
            var changes = remoteEntries
                .Where(entry => !requestingByKey.TryGetValue(entry.KeyHash, out var valueHash) || valueHash != entry.ValueHash)
                .Select(entry => new SyncChange(entry.KeyJson, entry.JsonObject, ChangeType.AddUpdated, entry.ValueHash))
                .ToArray();

            var response = new SyncChangesResponse(changes, remoteEntries.Select(entry => new SyncHash(entry.KeyHash, entry.ValueHash)).ToArray());

            return new SyncResponse("HASHES", JsonSerializer.Serialize(response));
        }
    }
    #endregion
}