

using Bejo.DistributedCollections.Dictionary;
using Bejo.DistributedCollections.Network;

namespace Bejo.DistributedCollections;


// Ansvarlig for at modtage settings og oprette data collections
public class ReplicationSet
{
    //private readonly ReplicationSetSettings _replicationSetSettings;
    private readonly Dictionary<string, IDistributedCollection> _distributedCollections = new();
    private readonly ReplicationManager _replicationManager;

    public ReplicationSet(ReplicationSetSettings replicationSetSettings)
    {
        //--_replicationSetSettings = replicationSetSettings;
        _replicationManager = new ReplicationManager(replicationSetSettings);
    }


    public IDictionary<TKey, TValue> GetOrCreateDistributedDictionary<TKey, TValue>(string name) where TKey : notnull
    {
        var keyType = typeof(TKey);
        if (!keyType.IsPrimitive && !keyType.IsEnum && keyType != typeof(string) &&
            keyType != typeof(Guid) && keyType != typeof(decimal) && keyType != typeof(DateTime) &&
            keyType != typeof(DateTimeOffset) && keyType != typeof(TimeSpan) &&
            keyType != typeof(DateOnly) && keyType != typeof(TimeOnly))
        {
            throw new NotSupportedException(
                $"Key type '{keyType}' is not supported. Use a non-nullable primitive, enum, string, Guid, or simple date/time type.");
        }

        // Check if a distributed collection with the given name already exists
        if(_distributedCollections.ContainsKey(name))
        {
            // If it exists, check if it is a distributed dictionary
            if(_distributedCollections[name] is IDictionary<TKey, TValue> dictionary)
            {
                // If it is, return it
                return dictionary;
            }
            // If it exists but is not a distributed dictionary, throw an exception
            throw new InvalidOperationException($"The collection with name '{name}' is not a distributed dictionary");
        }

        // If it does not exist, create a new distributed dictionary
        var node = new DistributedCollectionNode<TKey, TValue>(_replicationManager);
        var distributedDictionary = new DistributedDictionary<TKey, TValue>(name, node);
        _distributedCollections.Add(name, distributedDictionary);

        return distributedDictionary;
    }
}