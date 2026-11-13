

using Bejo.DistributedCollections.Dictionary;
using Bejo.DistributedCollections.Network;
using System.Text.Json;

namespace Bejo.DistributedCollections;


// Ansvarlig for at modtage settings og oprette data collections
public class ReplicationSet
{
    private readonly ReplicationSetSettings _replicationSetSettings;
    private readonly Dictionary<string, CollectionInfo> _distributedCollections = new();
    private readonly ReplicationManager _replicationManager;

    public ReplicationSet(ReplicationSetSettings replicationSetSettings)
    {
        _replicationSetSettings = replicationSetSettings;
        _replicationManager = new ReplicationManager(replicationSetSettings, _distributedCollections);
    }

    public IDictionary<TKey, TValue> GetOrCreateDistributedDictionary<TKey, TValue>(string name) where TKey : notnull
    {
        var keyType = typeof(TKey);
        if (!keyType.IsPrimitive && !keyType.IsEnum && keyType != typeof(string) &&   // TODO: Senere skal vi vælge hvilke typer vi understøtter
            keyType != typeof(Guid) && keyType != typeof(decimal) && keyType != typeof(DateTime) &&
            keyType != typeof(DateTimeOffset) && keyType != typeof(TimeSpan) &&
            keyType != typeof(DateOnly) && keyType != typeof(TimeOnly))
        {
            throw new NotSupportedException($"Key type '{keyType}' is not supported. Use a non-nullable primitive, enum, string, Guid, or simple date/time type.");
        }

        // Check if a distributed collection with the given name already exists
        if(_distributedCollections.ContainsKey(name))
        {
            // If it exists, check if it is a distributed dictionary
            if(_distributedCollections[name].DistributedCollection is IDictionary<TKey, TValue> dictionary)
            {
                // If it is, return it
                return dictionary;
            }
            // If it exists but is not a distributed dictionary, throw an exception
            throw new InvalidOperationException($"The collection with name '{name}' is not a distributed dictionary");
        }

        // If it does not exist, create a new distributed dictionary
        var node = new DistributedCollectionNode<TKey, TValue>(name, _replicationManager);
        
        var distributedDictionary = new DistributedDictionary<TKey, TValue>(node);
        
        _distributedCollections.Add(name, new CollectionInfo("DistributedDictionary", distributedDictionary, node));

        var jsonObject = JsonSerializer.Serialize(new DictionaryTypeInfo("DistributedDictionary",typeof(TKey).AssemblyQualifiedName,typeof(TValue).AssemblyQualifiedName));

        _replicationManager.SendCommand(new CommandDTO(name, jsonObject, ChangeType.CreateCollection, 0, 0));

        return distributedDictionary;
    }

    public (string Name, string CollectionType)[] GetAllCollections()
    {
        return _distributedCollections.Select(p => (p.Key, p.Value.CollectionType)).ToArray();
    }
}