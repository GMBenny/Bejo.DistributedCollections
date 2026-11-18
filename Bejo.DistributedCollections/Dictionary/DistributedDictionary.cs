using System.Collections;
using System.Diagnostics.CodeAnalysis;

namespace Bejo.DistributedCollections.Dictionary;

public class DistributedDictionary<TKey, TValue> : IDictionary<TKey, TValue>, IDistributedCollection where TKey : notnull
{
    // Reference to the distributed dictionary node that manages the underlying data.
    private readonly DistributedDictionaryNode<TKey, TValue> _node;

    public event EventHandler<DictionaryChangedEventArgs<TKey>>? Changed;

    // Kept internal, so only ReplicationSet can create instances of DistributedDictionary.
    internal DistributedDictionary(DistributedDictionaryNode<TKey, TValue> node)
    {
        _node = node;
        _node.Changed += HandleNodeChanged;
    }

    #region IDistributedCollection Members
    public TValue this[TKey key] 
    { 
        get => _node.TryGetValue(key, out var value) ? value : throw new KeyNotFoundException();
        
        set 
        {
            _node.TryCreateUpdate(key, value);
        }
    }

    public ICollection<TKey> Keys => _node.Keys;
    
    public ICollection<TValue> Values => _node.Values;

    public int Count => _node.Count;

    public bool IsReadOnly => false;

    public void Add(TKey key, TValue value)
    {
        if (!_node.TryAdd(key, value))
            throw new ArgumentException("An item with the same key has already been added.");
    }

    public void Add(KeyValuePair<TKey, TValue> item)
    {
        if (!_node.TryAdd(item.Key, item.Value))
            throw new ArgumentException("An item with the same key has already been added.");
    }

    public void Clear()
    {
        _node.Clear();
    }

    public bool Contains(KeyValuePair<TKey, TValue> item)
    {
        return _node.TryGetValue(item.Key, out var value) && EqualityComparer<TValue>.Default.Equals(value, item.Value);
    }

    public bool ContainsKey(TKey key)
    {
        return _node.TryGetValue(key, out _);
    }

    public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
    {
        ArgumentNullException.ThrowIfNull(array);
        if (arrayIndex < 0)
            throw new ArgumentOutOfRangeException(nameof(arrayIndex));

        var items = _node.GetSnapshot();
        if (array.Length - arrayIndex < items.Length)
            throw new ArgumentException("The destination array does not have enough space.", nameof(array));

        Array.Copy(items, 0, array, arrayIndex, items.Length);
    }

    public IDictionary<TKey, TValue> GetDictionary()
    {
        return new Dictionary<TKey, TValue>(_node.GetSnapshot());
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
    #endregion

    #region Additional Methods

    private void HandleNodeChanged(object? sender, DictionaryChangedEventArgs<TKey> eventArgs)
    {
        Changed?.Invoke(this, eventArgs);
    }

    #endregion
}