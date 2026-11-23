namespace Bejo.DistributedCollections;

public enum ThisStatus
{
    Starting,
    LoadingData,
    SyncingData,
    Online,
    Faulted,
    Rejected,
    Leaving,
    Stopped
}