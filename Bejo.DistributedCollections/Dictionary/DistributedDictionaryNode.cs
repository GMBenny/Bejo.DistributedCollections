using Bejo.DistributedCollections.Contracts;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;

namespace Bejo.DistributedCollections.Dictionary;

internal class DistributedDictionaryNode<TKey, TValue>(string name, string? persistPath, ICommandSender commandSender, ReplicationSet.HandleConflict<TKey, TValue> handleConflict) : IDistributedCollectionNode where TKey : notnull
{
    private ConcurrentDictionary<TKey, LocalStorage<TKey, TValue>> _dictionaryReplica = new();

    private ReplicationSet.HandleConflict<TKey, TValue> _handleConflict = handleConflict;

    internal ICollection<TKey> Keys => _dictionaryReplica.Keys;

    internal ICollection<TValue> Values => Array.AsReadOnly(_dictionaryReplica.Values.Select(v => v.Value).ToArray());

    internal int Count => _dictionaryReplica.Count;

    internal event EventHandler<DictionaryChangedEventArgs<TKey>>? Changed;

    internal KeyValuePair<TKey, TValue>[] GetSnapshot => _dictionaryReplica.Select(pair => new KeyValuePair<TKey, TValue>(pair.Key, pair.Value.Value)).ToArray();

    // get all key/value pairs as a snapshot in hash
    public SyncHash[] GetSyncHashes => _dictionaryReplica.Values.Select(value => new SyncHash(HashKey(value.Key), value.ValueHash)).ToArray();

    public SyncEntry[] GetSyncEntries => _dictionaryReplica.Values.Select(value => new SyncEntry(HashKey(value.Key), value.ValueHash, JsonSerializer.Serialize(value.Key), value.JsonString)).ToArray();

    internal void SetConflictHandler(ReplicationSet.HandleConflict<TKey, TValue> handleConflict)
    {
        _handleConflict = handleConflict;
    }

    internal bool TryAdd(TKey key, TValue value)
    {
        var localValue = new LocalStorage<TKey, TValue>(key, value);
        if (!_dictionaryReplica.TryAdd(key, localValue))
            return false;

        PersistDataToFile();

        commandSender.SendCommand(new CommandDTO(name, localValue.JsonString, ChangeType.AddUpdated, 0, localValue.ValueHash));
        return true;
    }

    internal bool TryCreateUpdate(TKey key, TValue value)
    {
        var localValue = new LocalStorage<TKey, TValue>(key, value);

        // If the key already exists, we update it and send a replication command
        if (_dictionaryReplica.TryGetValue(key, out var existingValue))
        {
            commandSender.SendCommand(new CommandDTO(name, localValue.JsonString, ChangeType.AddUpdated, existingValue.ValueHash, localValue.ValueHash));

            _dictionaryReplica[key] = localValue; // Update the value in the local dictionary

            PersistDataToFile();

            return true;
        }
        else // If the key does not exist, we create it and send a replication command
        {
            var createResult = _dictionaryReplica.TryAdd(key, localValue); // Attempt to add the new key-value pair

            // If local change was a success, then replicate the change.
            if (createResult)
                PersistDataToFile();

            if (createResult)
                commandSender.SendCommand(new CommandDTO(name, localValue.JsonString, ChangeType.AddUpdated, 0, localValue.ValueHash));
            
            return createResult;
        }
    }

    internal bool TryGetValue(TKey key, [MaybeNullWhen(false)] out TValue value)
    {
        if (_dictionaryReplica.TryGetValue(key, out var localStorage))
        {
            value = localStorage.Value;
            return true;
        }

        value = default;
        return false;
    }

    internal bool Remove(TKey key)
    {
        var removeResult = _dictionaryReplica.TryRemove(key, out var removedValue);

        // If local change was a success, then replicate the change.
        if (removeResult && removedValue != null)
        {
            PersistDataToFile();
            commandSender.SendCommand(new CommandDTO(name, removedValue.JsonString, ChangeType.Removed, removedValue.ValueHash, 0));
        }

        return removeResult;
    }

    internal void Clear()
    {
        _dictionaryReplica.Clear();
        PersistDataToFile();

        commandSender.SendCommand(new CommandDTO(name, string.Empty, ChangeType.ClearCollection, 0, 0));
    }



    public void CommandCallback(CommandDTO command)
    {
        switch (command.Action)
        {
            case ChangeType.AddUpdated:
                var l = new LocalStorage<TKey, TValue>(command.JsonObject);
                
                if (_dictionaryReplica.TryGetValue(l.Key, out var existingValue))
                {
                    if (existingValue.ValueHash == command.FromValueHash)
                    {
                        _dictionaryReplica[l.Key] = l;
                        PersistDataToFile();

                        // Fire event when data-change from remote source
                        Changed?.Invoke(this, new DictionaryChangedEventArgs<TKey>(l.Key, command.Action.ToString()));
                    }
                    else
                    {
                        // handle conflict using the provided delegate
                        var resolvedValue = _handleConflict(l.Key, new ConflictData<TValue>(existingValue.ValueHash, existingValue.LastChangedUTC, existingValue.Value), new ConflictData<TValue>(l.ValueHash, l.LastChangedUTC, l.Value));
                        _dictionaryReplica[l.Key] = new LocalStorage<TKey, TValue>(l.Key, resolvedValue);
                        PersistDataToFile();

                        // Fire event when data-change from remote source
                        Changed?.Invoke(this, new DictionaryChangedEventArgs<TKey>(l.Key, command.Action.ToString()));
                    }
                }
                else
                {
                    _dictionaryReplica.TryAdd(l.Key, l);
                    PersistDataToFile();

                    // Fire event when data-change from remote source
                    Changed?.Invoke(this, new DictionaryChangedEventArgs<TKey>(l.Key, command.Action.ToString()));
                }

                break;
            case ChangeType.Removed:
                var removed = new LocalStorage<TKey, TValue>(command.JsonObject);
                _dictionaryReplica.TryRemove(removed.Key, out _);
                PersistDataToFile();

                Changed?.Invoke(this, new DictionaryChangedEventArgs<TKey>(removed.Key, command.Action.ToString())); // Notify that the item has been removed
                
                break;
            case ChangeType.ClearCollection:
                _dictionaryReplica.Clear();
                PersistDataToFile();

                Changed?.Invoke(this, new DictionaryChangedEventArgs<TKey>(default!, command.Action.ToString())); // Notify that the collection has been cleared

                break;
            default:
                throw new InvalidOperationException($"Unsupported command action: {command.Action}");
        }
    }

    public void ApplySyncChanges(SyncChange[] changes, SyncHash[] remoteHashes)
    {
        var remoteKeyHashes = remoteHashes.Select(hash => hash.KeyHash).ToHashSet();
        var allChanges = changes.ToList();

        foreach (var local in _dictionaryReplica.Values.ToArray())
        {
            // A local key that does not exist in the remote snapshot is treated as removed.
            if (!remoteKeyHashes.Contains(HashKey(local.Key)) && _dictionaryReplica.ContainsKey(local.Key))
                allChanges.Add(new SyncChange(JsonSerializer.Serialize(local.Key), null, ChangeType.Removed, 0));
        }

        // Apply the changes to the local collection.
        foreach (var change in allChanges)
        {
            var key = JsonSerializer.Deserialize<TKey>(change.KeyJson) ?? throw new InvalidOperationException("Sync change contained an invalid key.");

            if (change.ChangeType == ChangeType.AddUpdated)
            {
                if (change.JsonObject == null)
                    throw new InvalidOperationException("An add/update sync change must contain an object.");

                var local = new LocalStorage<TKey, TValue>(change.JsonObject);
                if (!EqualityComparer<TKey>.Default.Equals(key, local.Key))
                    throw new InvalidOperationException("Sync change key does not match its object.");
                if (local.ValueHash != change.ValueHash)
                    throw new InvalidOperationException($"Sync hash validation failed for key '{local.Key}'.");

                // Update
                _dictionaryReplica[local.Key] = local;

                // TODO: Should we fire the Changed event for each change? This could be a lot of events if there are many changes.
                //Changed?.Invoke(this, new DictionaryChangedEventArgs<TKey>(local.Key, change.ChangeType.ToString()));
            }
            else if (change.ChangeType == ChangeType.Removed)
            {
                _dictionaryReplica.TryRemove(key, out _);
                // TODO: Should we fire the Changed event for each change? This could be a lot of events if there are many changes.
                //Changed?.Invoke(this, new DictionaryChangedEventArgs<TKey>(key, change.ChangeType.ToString()));
            }
            else
            {
                throw new InvalidOperationException($"Unsupported sync action: {change.ChangeType}.");
            }
        }

        PersistDataToFile();

        var actualHashes = GetSyncHashes.OrderBy(hash => hash.KeyHash).ThenBy(hash => hash.ValueHash).ToArray();
        var expectedHashes = remoteHashes.OrderBy(hash => hash.KeyHash).ThenBy(hash => hash.ValueHash).ToArray();

        if (!actualHashes.SequenceEqual(expectedHashes))
            throw new InvalidOperationException("Sync validation failed after applying changes.");
    }

    private static ulong HashKey(TKey key)
    {
        return Helpers.Helper.Hash(JsonSerializer.Serialize(key));
    }

    public void LoadDataFromFile()
    {
        // As it's a new collection, load its data if any is found
        if (persistPath != null)
        {
            var data = Helpers.Helper.LoadFromFile($"DistributedDictionary_{Helpers.Helper.Hash(name)}", persistPath, new ConcurrentDictionary<TKey, LocalStorage<TKey, TValue>>());

            if(_dictionaryReplica.Count > 0)
                throw new InvalidOperationException("The collection already exists and has data, but it should be a new collection.");

            _dictionaryReplica = data;
        }
    }
    public void PersistDataToFile()
    {
        if (persistPath != null)
        {
            Helpers.Helper.SaveToFile($"DistributedDictionary_{Helpers.Helper.Hash(name)}", persistPath, _dictionaryReplica);
        }
    }
}