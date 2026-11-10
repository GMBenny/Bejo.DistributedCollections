

namespace Bejo.DistributedCollections;

public class ReplicationSet
{
    private ReplicationSetSettings _replicationSetSettings;


    public ReplicationSet(ReplicationSetSettings replicationSetSettings)
    {
        _replicationSetSettings = replicationSetSettings;
    }


    public IDictionary<string, TValue> GetOrCreateDistributedDictionary<TValue>(string name)
    {
        return new Dictionary<string, TValue>();

    }
}