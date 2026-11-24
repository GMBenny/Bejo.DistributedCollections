namespace Bejo.DistributedCollections.Contracts;

internal interface IMembershipReceiver
{
    void HandleMembership(MembershipUpdate update);
}
