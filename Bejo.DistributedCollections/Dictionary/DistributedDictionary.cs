using System.Collections;
using System.Diagnostics.CodeAnalysis;

namespace Bejo.DistributedCollections.Dictionary;

public class DistributedDictionary<TKey, TValue> : IDictionary<TKey, TValue>, IDistributedCollection
    where TKey : notnull
{
    private readonly string _name;

    private readonly DistributedCollectionNode<TKey, TValue> _node;

    internal DistributedDictionary(string name, DistributedCollectionNode<TKey, TValue> node)
    {
        _name = name;
        _node = node;
    }

    public TValue this[TKey key] 
    { 
        get => _node.TryGetValue(key, out var value) ? value : throw new KeyNotFoundException();
        set => _node.Add(key, value);
    }

    public ICollection<TKey> Keys => _node.Keys;
    
    public ICollection<TValue> Values => _node.Values;

    public int Count => _node.Keys.Count;

    public bool IsReadOnly => false;

    public void Add(TKey key, TValue value)
    {
        _node.Add(key, value);
    }

    public void Add(KeyValuePair<TKey, TValue> item)
    {
        _node.Add(item.Key, item.Value);
    }

    public void Clear()
    {
        throw new NotImplementedException();
    }

    public bool Contains(KeyValuePair<TKey, TValue> item)
    {
        throw new NotImplementedException();
    }

    public bool ContainsKey(TKey key)
    {
        throw new NotImplementedException();
    }

    public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
    {
        foreach (var key in _node.Keys)
        {
            array[arrayIndex++] = new KeyValuePair<TKey, TValue>(key, _node.TryGetValue(key, out var value) ? value : throw new KeyNotFoundException());
        }
    }

    public IDictionary<TKey, TValue> GetDictionary()
    {
        var dict = new Dictionary<TKey, TValue>(_node.Keys.Count);
        foreach (var key in _node.Keys)
        {
            dict[key] = _node.TryGetValue(key, out var value) ? value : throw new KeyNotFoundException();
        }
        return dict;
    }

    public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
    {
        return GetDictionary().GetEnumerator();
    }

    public bool Remove(TKey key)
    {
        return _node.Remove(key);
    }

    public bool Remove(KeyValuePair<TKey, TValue> item)
    {
        return _node.TryGetValue(item.Key, out var value) && EqualityComparer<TValue>.Default.Equals(value, item.Value) && _node.Remove(item.Key);
    }

    public bool TryGetValue(TKey key, [MaybeNullWhen(false)] out TValue value)
    {
        return _node.TryGetValue(key, out value);
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }
}