using Bejo.DistributedCollections.Contracts;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;

namespace Bejo.DistributedCollections.Dictionary;

internal class DistributedDictionaryNode<TKey, TValue>(string name, string? persistPath, ICommandSender commandSender, ReplicationSet.HandleConflict<TKey, TValue> handleConflict) : IDistributedCollectionNode where TKey : notnull
{
    private ConcurrentDictionary<TKey, LocalStorage<TKey, TValue>> _dictionaryReplica = new();
    // AI: TODO: Load this dictionary replica from its persistent file during construction, before any remote synchronization, and persist it after every local or remote change.
    private ReplicationSet.HandleConflict<TKey, TValue> _handleConflict = handleConflict;

    internal ICollection<TKey> Keys => _dictionaryReplica.Keys;

    // CoPilot: Expose values as a read-only snapshot because values cannot be added or removed independently of keys.
    internal ICollection<TValue> Values => Array.AsReadOnly(_dictionaryReplica.Values.Select(v => v.Value).ToArray());

    internal int Count => _dictionaryReplica.Count;

    internal event EventHandler<DictionaryChangedEventArgs<TKey>>? Changed;

    internal KeyValuePair<TKey, TValue>[] GetSnapshot() => _dictionaryReplica.Select(pair => new KeyValuePair<TKey, TValue>(pair.Key, pair.Value.Value)).ToArray();

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
        // AI: TODO: Persist the dictionary after this local removal.
        if (removeResult && removedValue != null)
            commandSender.SendCommand(new CommandDTO(name, removedValue.JsonString, ChangeType.Removed, removedValue.ValueHash, 0));

        return removeResult;
    }

    internal void Clear()
    {
        _dictionaryReplica.Clear();

        // AI: TODO: Persist the dictionary after this local clear operation.
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
                        // AI: TODO: Persist the dictionary after applying a remote update that matched the local hash.

                        // Fire event when data-change from remote source
                        Changed?.Invoke(this, new DictionaryChangedEventArgs<TKey>(l.Key, command.Action.ToString()));
                    }
                    else
                    {
                        // handle conflict using the provided delegate
                        var resolvedValue = _handleConflict(l.Key, new ConflictData<TValue>(existingValue.ValueHash, existingValue.LastChangedUTC, existingValue.Value), new ConflictData<TValue>(l.ValueHash, l.LastChangedUTC, l.Value));
                        _dictionaryReplica[l.Key] = new LocalStorage<TKey, TValue>(l.Key, resolvedValue);
                        // AI: TODO: Persist the dictionary after resolving and applying a remote conflict.

                        // Fire event when data-change from remote source
                        Changed?.Invoke(this, new DictionaryChangedEventArgs<TKey>(l.Key, command.Action.ToString()));
                    }
                }
                else
                {
                    _dictionaryReplica.TryAdd(l.Key, l);
                    // AI: TODO: Persist the dictionary after applying a remote item that was missing locally.

                    // Fire event when data-change from remote source
                    Changed?.Invoke(this, new DictionaryChangedEventArgs<TKey>(l.Key, command.Action.ToString()));
                }

                break;
            case ChangeType.Removed:
                var removed = new LocalStorage<TKey, TValue>(command.JsonObject);
                _dictionaryReplica.TryRemove(removed.Key, out _);
                // AI: TODO: Persist the dictionary after applying a remote removal.
                // TODO: Fyr event af, så brugeren ved dette objekt er blevet slettet via netwærket.
                break;
            case ChangeType.ClearCollection:
                _dictionaryReplica.Clear();
                // AI: TODO: Persist the dictionary after applying a remote clear operation.
                // TODO: Fyr event af, så brugeren ved hele collectionen er blevet ryddet via netwærket.
                break;
            default:
                throw new InvalidOperationException($"Unsupported command action: {command.Action}");
        }
    }

    public void LoadDataFromFile()
    {
        // As it's a new collection, load it's data if any is found
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