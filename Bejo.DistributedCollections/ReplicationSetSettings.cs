namespace Bejo.DistributedCollections;

// Settings
public record ReplicationSetSettings(string nodeName, int listenerPort, bool compressData, string[]? masterKeys, NodeSettings[] nodes, int retryCount, TimeSpan timeout, string? persistPath = null, string? nodeKey = null);

public record NodeSettings(string Host, int Port, TimeSpan PingInterval);