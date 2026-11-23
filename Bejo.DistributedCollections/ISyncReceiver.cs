namespace Bejo.DistributedCollections;

internal interface ISyncReceiver
{
    SyncResponse HandleSyncRequest(SyncRequest request);
}
