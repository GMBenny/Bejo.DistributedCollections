namespace Bejo.DistributedCollections;

internal sealed record MembershipMember(string NodeName, string Host, int Port);

internal sealed record MembershipUpdate(
    long Version,
    MembershipMember[] Members,
    MembershipMember[] RemovedMembers);
