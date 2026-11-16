namespace Bejo.DistributedCollections;

// Settings
public record ReplicationSetSettings(string nodeName, int listenerPort, bool compressData, string[]? passwords, NodeSettings[] nodes, int retryCount, TimeSpan timeout);

public record NodeSettings(string Host, int Port, TimeSpan PingInterval);