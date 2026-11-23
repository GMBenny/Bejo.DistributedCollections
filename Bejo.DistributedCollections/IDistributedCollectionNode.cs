using Bejo.DistributedCollections.Contracts;

namespace Bejo.DistributedCollections;

internal interface IDistributedCollectionNode
{
    void CommandCallback(CommandDTO command);
    void LoadDataFromFile();
    SyncHash[] GetSyncHashes { get; }
    SyncEntry[] GetSyncEntries { get; }
    void ApplySyncChanges(SyncChange[] changes, SyncHash[] remoteHashes);
}