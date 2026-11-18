namespace Bejo.DistributedCollections.Dictionary;

public class DictionaryChangedEventArgs<TKey> : EventArgs
{
    public TKey Key { get; }
    public string Action { get; }

    public DictionaryChangedEventArgs(TKey key, string action)
    {
        Key = key;
        Action = action;
    }
}