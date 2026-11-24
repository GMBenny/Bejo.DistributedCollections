using Bejo.DistributedCollections;

// Information about a distributed collection, including its type, the collection itself, and the node it belongs to.
internal record CollectionInfo(string CollectionType, IDistributedCollection DistributedCollection, IDistributedCollectionNode Node, string RecreateCollectionMetadata);