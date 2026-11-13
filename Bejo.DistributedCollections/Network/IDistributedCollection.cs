using Bejo.DistributedCollections.Network;


// TODO: Sikke noget rod - Det skal rydes op

internal interface IDistributedCollection
{
}


internal interface IDistributedCollectionNode
{
    void CommandCallback(CommandDTO command);
}

internal record CollectionInfo(string CollectionType, IDistributedCollection DistributedCollection, IDistributedCollectionNode Node);


public record DictionaryTypeInfo(string CollectionType, string KeyType, string ValueType);