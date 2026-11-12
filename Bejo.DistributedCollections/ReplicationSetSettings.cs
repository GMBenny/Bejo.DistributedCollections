namespace Bejo.DistributedCollections;

// Settings
public record ReplicationSetSettings(int listenerPort, bool compressData, string[]? passwords, bool compress, NodeSettings[] nodes, int retryCount, TimeSpan timeout);

public record NodeSettings(string Host, int Port, TimeSpan PingInterval, bool Synchronous);