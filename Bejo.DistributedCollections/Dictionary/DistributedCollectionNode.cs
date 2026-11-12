using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using Bejo.DistributedCollections.Network;

namespace Bejo.DistributedCollections.Dictionary;

internal class DistributedCollectionNode<TKey, TValue>(string name, ReplicationManager replicationManager) where TKey : notnull
{
    private readonly ConcurrentDictionary<TKey, LocalStorage<TKey, TValue>> _dictionaryReplica = new();

    internal ICollection<TKey> Keys => _dictionaryReplica.Keys;

    internal ICollection<TValue> Values => _dictionaryReplica.Values.Select(v => v.Value).ToList();

    internal bool TryCreateUpdate(TKey key, TValue value)
    {
        var localValue = new LocalStorage<TKey, TValue>(key, value);

        // If the key already exists, we update it and send a replication command
        if (_dictionaryReplica.TryGetValue(key, out var existingValue))
        {
            replicationManager.SendCommand(new ChangeDTO(name, localValue.JsonString, ChangeType.AddUpdated, existingValue.ValueHash, localValue.ValueHash));

            _dictionaryReplica[key] = localValue; // Update the value in the local dictionary

            return true;
        }
        else // If the key does not exist, we create it and send a replication command
        {
            var createResult = _dictionaryReplica.TryAdd(key, localValue); // Attempt to add the new key-value pair

            // If local change was a success, then replicate the change.
            if (createResult)
                replicationManager.SendCommand(new ChangeDTO(name, localValue.JsonString, ChangeType.AddUpdated, 0, localValue.ValueHash));
            
            return createResult;
        }
    }

    internal bool TryGetValue(TKey key, [MaybeNullWhen(false)] out TValue value)
    {
        return _dictionaryReplica.TryGetValue(key, out var localStorage) ? (value = localStorage.Value) != null : (value = default) != null;
    }

    internal bool Remove(TKey key)
    {
        var removeResult = _dictionaryReplica.TryRemove(key, out var removedValue);

        // If local change was a success, then replicate the change.
        if (removeResult && removedValue != null)
            replicationManager.SendCommand(new ChangeDTO(name, removedValue.JsonString, ChangeType.Removed, removedValue.ValueHash, 0));

        return removeResult;
    }

    internal void Clear()
    {
        _dictionaryReplica.Clear();

        replicationManager.SendCommand(new ChangeDTO(name, string.Empty, ChangeType.ClearCollection, 0, 0));
    }
}