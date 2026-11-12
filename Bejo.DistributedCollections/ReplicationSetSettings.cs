namespace Bejo.DistributedCollections;

// Settings
public record ReplicationSetSettings(int listenerPort, bool compressData, string[] passwords, NodeSettings[] nodes);

public record NodeSettings(string Host, int Port, TimeSpan PingInterval, bool Synchronous);