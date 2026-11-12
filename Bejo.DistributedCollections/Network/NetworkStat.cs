internal enum NetworkStatus {Online, UnexpectedResult, Offline, TimeProblem}

internal record NetworkStat(DateTime LastRoundtripUTC, NetworkStatus LastStatus, TimeSpan RoundtripTime);