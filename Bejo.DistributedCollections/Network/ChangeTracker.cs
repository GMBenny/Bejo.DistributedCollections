

namespace Bejo.DistributedCollections.Network;

internal class ChangeTracker
{
    private readonly NodeSettings _nodeSettings;

    public ChangeTracker(NodeSettings nodeSettings)
    {
        _nodeSettings = nodeSettings;
    }
}