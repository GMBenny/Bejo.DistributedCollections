internal enum NetworkStatus {Online, UnexpectedResult, Offline, TimeProblem}

internal record NetworkStat(DateTime LastRoundtripUTC, NetworkStatus LastStatus, TimeSpan RoundtripTime);

public record PublicNetworkStat(string Address, TimeSpan RoundtripTime, string LastStatus, DateTime LastRoundtripUTC);
