namespace Bejo.DistributedCollections;

// Settings
public record ReplicationSetSettings(
    string nodeName,  // Give each node a unique name to identify it in the replication set
    int listenerPort, // Port to listen for incoming connections from other nodes
    bool compressData, // Whether to compress data before sending it over the network
    string[]? masterKeys, // Optional keys for encryption (First for encryption and first to last for decryption)
    NodeSettings[] nodes, // Known nodes in the replication set
    int retryCount, // Number of times to retry sending data to a node before giving up
    TimeSpan timeout, // Timeout for network operations
    string? persistPath = null, // Optional path to persist data to disk
    string? nodeKey = null // Optional list of keys for node authentication
);

public record NodeSettings(
    string Host, // Hostname or IP address of the node
    int Port, // Port to connect to the node
    TimeSpan PingInterval // Interval at which to ping the node to check if it's alive
);