namespace Bejo.DistributedCollections.Contracts;

internal interface ICommandReceiver
{
    void HandleCommand(CommandDTO command);
}
