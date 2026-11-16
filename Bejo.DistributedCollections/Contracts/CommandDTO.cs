namespace Bejo.DistributedCollections.Contracts;

internal enum ChangeType { Ignored = 0, CreateCollection = 1, AddUpdated = 2, Removed = 3, ClearCollection = 4, RemoveCollection = 5, Message = 6 }
internal class CommandDTO
{
    public string CollectionName { get; private set; } // If command is intented for a collection

    public ChangeType Action { get; private set; } // The type of change this command represents

    public string JsonObject { get; private set; } // If the command contains data

    public DateTime TimestampUTC { get; private set; } // The timestamp when the command was created
    public ulong FromValueHash { get; private set; } // The hash of the local-value before the change
    public ulong ToValueHash { get; private set; } // The hash of the local-value after the change

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