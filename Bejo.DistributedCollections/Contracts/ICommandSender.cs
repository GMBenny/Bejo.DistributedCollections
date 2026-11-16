namespace Bejo.DistributedCollections.Contracts;

internal interface ICommandSender
{
    void SendCommand(CommandDTO command);
}
