using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using Bejo.DistributedCollections.Network;


namespace Bejo.DistributedCollections.Dictionary;

internal class DistributedCollectionNode<TKey, TValue>(ReplicationManager replicationManager) where TKey : notnull
{
    private readonly ConcurrentDictionary<TKey, TValue> _dictionaryReplica = new();

    internal ICollection<TKey> Keys => _dictionaryReplica.Keys;

    internal ICollection<TValue> Values => _dictionaryReplica.Values;

    internal bool TryCreate(TKey key, TValue value)
    {
        return _dictionaryReplica.TryAdd(key, value);
    }

    internal bool TryGetValue(TKey key, [MaybeNullWhen(false)] out TValue value)
    {
        return _dictionaryReplica.TryGetValue(key, out value);
    }

    internal void Add(TKey key, TValue value)
    {
        _dictionaryReplica[key] = value;
    }
    internal bool Remove(TKey key)
    {
        return _dictionaryReplica.TryRemove(key, out _);
    }
}