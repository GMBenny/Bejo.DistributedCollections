using Bejo.DistributedCollections.Contracts;

namespace Bejo.DistributedCollections;

internal interface IDistributedCollectionNode
{
    void CommandCallback(CommandDTO command);
    void LoadDataFromFile();
}