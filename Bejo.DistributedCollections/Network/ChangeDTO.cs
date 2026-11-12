namespace Bejo.DistributedCollections.Network;

internal enum ChangeType { Ignored = 0, Added = 1, Updated = 2, Removed = 3, ClearCollection = 9 }
internal class ChangeDTO
{
    public string JsonObject { get; private set; }

    public ChangeType Action { get; private set; }

    public ulong FromValueHash { get; private set; }
    public ulong ToValueHash { get; private set; }

    public ChangeDTO(string jsonObject, ChangeType action, ulong fromValueHash, ulong toValueHash)
    {
        JsonObject = jsonObject;
        Action = action;
        FromValueHash = fromValueHash;
        ToValueHash = toValueHash;
    }
}