using Bejo.DistributedCollections.Dictionary;
using Bejo.DistributedCollections.Contracts;
using Bejo.DistributedCollections.Network;
using System.Reflection;
using System.Text.Json;

namespace Bejo.DistributedCollections;


// Manages a set of distributed collections and handles replication commands
public class ReplicationSet : ICommandReceiver
{
    // Local storage for all distributed collections, keyed by collection name
    private readonly Dictionary<string, CollectionInfo> _distributedCollections = new();
    private readonly Dictionary<string, string> _recreateCollectionMetadata = new();

    // ReplicationSet settings
    private ReplicationSetSettings _replicationSetSettings;

    // Manages server and client communication for replication
    private readonly ReplicationManager _replicationManager;

    // Low-coupling interface for sending commands to other nodes in the replication set
    private readonly ICommandSender _commandSender;

    private readonly Action<string>? _messageHandler;


    // Create a new ReplicationSet with the specified settings, initializing the replication manager and command sender
    public ReplicationSet(ReplicationSetSettings replicationSetSettings, Action<string>? messageHandler = null)
    {
        ArgumentNullException.ThrowIfNull(replicationSetSettings);

        if (replicationSetSettings.persistPath != null && !Directory.Exists(replicationSetSettings.persistPath))
            throw new ArgumentException("Persist path must exist or be null.", nameof(replicationSetSettings.persistPath));

        _replicationSetSettings = replicationSetSettings;
        _replicationManager = new ReplicationManager(replicationSetSettings, this);
        _commandSender = _replicationManager;
        _messageHandler = messageHandler;

        // TODO: Set status

        // Ikke helt godt nok til at skabe en brugbar collection, da vi ikke har synkroniseret med andre noder endnu. Vi skal vente på at alle noder er startet og har synkroniseret deres collections.
        if (replicationSetSettings.persistPath != null)
        {
            _recreateCollectionMetadata = Helpers.Helper.LoadFromFile("DistributedCollections", replicationSetSettings.persistPath, new Dictionary<string, string>());

            foreach (var collectionMetadata in _recreateCollectionMetadata)
            {
                var dictionaryTypeInfo = JsonSerializer.Deserialize<DictionaryTypeInfo>(collectionMetadata.Value);
                if(dictionaryTypeInfo?.CollectionType == "DistributedDictionary")
                    ValidateAndCreateDistributedDictionary(collectionMetadata.Key, dictionaryTypeInfo);
            }

        }

        // TODO: Set status

        _replicationManager.Start();

        // TODO: Set status

        // AI: TODO: After the replicaset-level collection comparison is complete, ask every matching remote data collection for its key and value hash set, then request any missing or stale entries.

        // TODO: Set status to ready as this node has read from file and synced from other nodes. The collections are now ready for use.
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
        var thisStatus = _replicationManager?.ServerState switch
        {
            Server.ServerState.Starting => ThisStatus.Starting,
            Server.ServerState.Started => ThisStatus.Started,
            Server.ServerState.Faulted => ThisStatus.Faulted,
            _ => ThisStatus.Stopped
        };
        return new ReplicationSetStatus(_replicationSetSettings.nodeName, thisStatus, _replicationManager?.GetNetworkStats() ?? Array.Empty<NetworkStat>());
    }

    // Send a message to all nodes in the replication set
    public async Task SendMessage(string message)
    {
        await _replicationManager.SendMessage(message);
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

            var collection = CreateDictionary<TKey, TValue>(name, handleConflict);
            _distributedCollections.Add(name, collection);
            
            var collectionMetadata = JsonSerializer.Serialize(new DictionaryTypeInfo("DistributedDictionary", typeof(TKey).AssemblyQualifiedName!, typeof(TValue).AssemblyQualifiedName!));

            // Update with metadata
            _recreateCollectionMetadata.Add(name, collectionMetadata);
            if (_replicationSetSettings.persistPath != null)
                Helpers.Helper.SaveToFile("DistributedCollections", _replicationSetSettings.persistPath, _recreateCollectionMetadata);

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

    private void ValidateAndCreateDistributedDictionary(string collectionName, DictionaryTypeInfo dictionaryTypeInfo)
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
        var collection = (CollectionInfo)factory.Invoke(this, new object[] { collectionName })!;
        _distributedCollections.Add(collectionName, collection);

        _distributedCollections[collectionName].Node.LoadDataFromFile();
    }

    private CollectionInfo CreateDictionary<TKey, TValue>(string name, HandleConflict<TKey, TValue> handleConflict) where TKey : notnull
    {
        var node = new DistributedDictionaryNode<TKey, TValue>(name, _replicationSetSettings.persistPath, _commandSender, handleConflict);
        var dictionary = new DistributedDictionary<TKey, TValue>(node);
        return new CollectionInfo("DistributedDictionary", dictionary, node);
    }

    private CollectionInfo CreateRemoteDictionary<TKey, TValue>(string name) where TKey : notnull
    {
        return CreateDictionary<TKey, TValue>(name, (key, localValue, distributedValue) => localValue.Value);
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
                    ValidateAndCreateDistributedDictionary(command.CollectionName, dictionaryTypeInfo);
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
    #endregion
}