namespace Bejo.DistributedCollections;

public record ConflictData<TValue>(ulong HashCode, DateTime CreatedTimestamp, TValue Value);