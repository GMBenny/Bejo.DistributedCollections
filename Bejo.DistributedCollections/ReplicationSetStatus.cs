namespace Bejo.DistributedCollections;

public record ReplicationSetStatus(
    string NodeName,
    ThisStatus ThisStatus,
    NetworkStat[] NetworkStats,
    bool ServerStarted);