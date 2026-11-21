public enum NetworkStatus {
    Offline, // The node is offline
    Online, // This node may be used for replication
    LoadingData, // This node is loading data from file and should not be used for replication
    Syncing, // This node is syncing data from another node and should not be used for replication
    UnexpectedResult, // Some unexpected result was returned from the node
    TimeProblem, // The node is online, but the time is not in sync with this node
    NoNodeKey // The node have not been accepted -> Connected but no nodekey recieved from the node - When the node key comes: If node key er ok then the status should be LoadingData, Syncing, Online | If the node key is not ok the node is cut off and the node removed from ReplicationManager._commandhandlers and thereby from GetNetworkStats()
}

public record NetworkStat(string Address, DateTime LastRoundtripUTC, NetworkStatus LastStatus, TimeSpan RoundtripTime);

public record PublicNetworkStat(string Address, TimeSpan RoundtripTime, string LastStatus, DateTime LastRoundtripUTC);
