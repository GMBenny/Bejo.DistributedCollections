public enum NetworkStatus {Online, UnexpectedResult, Offline, TimeProblem}

public record NetworkStat(string Address, DateTime LastRoundtripUTC, NetworkStatus LastStatus, TimeSpan RoundtripTime);

public record PublicNetworkStat(string Address, TimeSpan RoundtripTime, string LastStatus, DateTime LastRoundtripUTC);
