using Bejo.DistributedCollections.Contracts;

namespace Bejo.DistributedCollections;

internal sealed record SyncRequest(string Action, string? CollectionName, string JsonPayload);

internal sealed record SyncResponse(string Action, string JsonPayload);

internal sealed record SyncCollectionMetadata(string Name, string CollectionType, string Metadata);

internal sealed record SyncHash(ulong KeyHash, ulong ValueHash);

internal sealed record SyncEntry(ulong KeyHash, ulong ValueHash, string KeyJson, string JsonObject);

internal sealed record SyncChange(string KeyJson, string? JsonObject, ChangeType ChangeType, ulong ValueHash);

internal sealed record SyncChangesResponse(SyncChange[] Changes, SyncHash[] RemoteHashes);
