namespace Bejo.DistributedCollections.Dictionary;

// Used to create a dictionary on the server side
public record DictionaryTypeInfo(string CollectionType, string KeyType, string ValueType);