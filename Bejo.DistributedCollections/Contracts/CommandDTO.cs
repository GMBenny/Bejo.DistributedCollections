namespace Bejo.DistributedCollections.Contracts;

internal enum ChangeType { Ignored = 0, CreateCollection = 1, AddUpdated = 2, Removed = 3, ClearCollection = 4, RemoveCollection = 5 }
internal class CommandDTO
{
    public string CollectionName { get; private set; }

    public string JsonObject { get; private set; }

    public ChangeType Action { get; private set; }

    public DateTime TimestampUTC { get; private set; }
    public ulong FromValueHash { get; private set; }
    public ulong ToValueHash { get; private set; }

    public CommandDTO(string collectionName, string jsonObject, ChangeType action, ulong fromValueHash, ulong toValueHash)
    {
        TimestampUTC = DateTime.UtcNow;
        CollectionName = collectionName;
        JsonObject = jsonObject;
        Action = action;
        FromValueHash = fromValueHash;
        ToValueHash = toValueHash;
    }
}