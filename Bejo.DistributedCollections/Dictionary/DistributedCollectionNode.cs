using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using Bejo.DistributedCollections.Network;

namespace Bejo.DistributedCollections.Dictionary;

internal class DistributedCollectionNode<TKey, TValue>(string name, ReplicationManager replicationManager) where TKey : notnull
{
    private readonly ConcurrentDictionary<TKey, LocalStorage<TKey, TValue>> _dictionaryReplica = new();

    internal ICollection<TKey> Keys => _dictionaryReplica.Keys;

    internal ICollection<TValue> Values => _dictionaryReplica.Values.Select(v => v.Value).ToList();

    internal bool TryCreate(TKey key, TValue value)
    {
        var localValue = new LocalStorage<TKey, TValue>(key, value);

        var createResult = _dictionaryReplica.TryAdd(key, localValue);

        // If local change was a success, then replicate the change.
        if (createResult)
            replicationManager.SendCommand(name, new ChangeDTO(localValue.JsonString, ChangeType.Added, 0, localValue.ValueHash));

        return createResult;
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
            replicationManager.SendCommand(name, new ChangeDTO(removedValue.JsonString, ChangeType.Removed, removedValue.ValueHash, 0));

        return removeResult;
    }

    internal void Clear()
    {
        _dictionaryReplica.Clear();

        replicationManager.SendCommand(name, new ChangeDTO(string.Empty, ChangeType.ClearCollection, 0, 0));
    }
}