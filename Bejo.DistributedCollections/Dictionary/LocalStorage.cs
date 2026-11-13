using Bejo.DistributedCollections.Network;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Bejo.DistributedCollections.Dictionary;

internal class LocalStorage<TKey, TValue>
{
    private DataObject _dataObject;

    public TKey Key => _dataObject.Key;
    public TValue Value => _dataObject.Value;
    public DateTime LastChangedUTC => _dataObject.LastChangedUTC;
    public string JsonString { get; private set; }

    public ulong ValueHash { get; }

    public LocalStorage(TKey key, TValue value)
    {
        _dataObject = new DataObject(key, value, DateTime.UtcNow);

        JsonString = JsonSerializer.Serialize(_dataObject);
        ValueHash = Helpers.Helper.Hash(JsonString);
    }

    public LocalStorage(string jsonObject)
    {
        _dataObject = JsonSerializer.Deserialize<DataObject>(jsonObject) ?? throw new InvalidOperationException("Failed to deserialize JSON object.");
        JsonString = jsonObject;
        ValueHash = Helpers.Helper.Hash(JsonString);
    }

    internal class DataObject
    {
        public TKey Key { get; }
        public TValue Value { get; }
        public DateTime LastChangedUTC { get; }

        public DataObject(TKey key, TValue value, DateTime lastChangedUTC)
        {
            Key = key;
            Value = value;
            LastChangedUTC = lastChangedUTC;
        }
    }
}