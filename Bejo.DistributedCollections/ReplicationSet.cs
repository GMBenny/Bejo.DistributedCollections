using Bejo.DistributedCollections.Dictionary;
using Bejo.DistributedCollections.Contracts;
using Bejo.DistributedCollections.Network;
using System.Reflection;
using System.Text.Json;

namespace Bejo.DistributedCollections;


// Ansvarlig for at modtage settings og oprette data collections
public class ReplicationSet : ICommandReceiver
{
    private readonly Dictionary<string, CollectionInfo> _distributedCollections = new();
    
    private readonly ICommandSender _commandSender;

    public ReplicationSet(ReplicationSetSettings replicationSetSettings)
    {
        var replicationManager = new ReplicationManager(replicationSetSettings, this);
        _commandSender = replicationManager;
        replicationManager.Start();
    }

    internal ReplicationSet(ICommandSender commandSender)
    {
        _commandSender = commandSender;
    }

    public IDictionary<TKey, TValue> GetOrCreateDistributedDictionary<TKey, TValue>(string name) where TKey : notnull
    {
        ValidateKeyType(typeof(TKey));

        lock (_collectionsLock)
        {
            if (_distributedCollections.TryGetValue(name, out var existing))
            {
                if (existing.DistributedCollection is IDictionary<TKey, TValue> dictionary)
                    return dictionary;

                throw new InvalidOperationException($"The collection with name '{name}' is not a distributed dictionary");
            }

            var collection = CreateDictionary<TKey, TValue>(name);
            _distributedCollections.Add(name, collection);

            var typeInfo = new DictionaryTypeInfo("DistributedDictionary", typeof(TKey).AssemblyQualifiedName!, typeof(TValue).AssemblyQualifiedName!);
            var jsonObject = JsonSerializer.Serialize(typeInfo);
            _commandSender.SendCommand(new CommandDTO(name, jsonObject, ChangeType.CreateCollection, 0, 0));

            return (IDictionary<TKey, TValue>)collection.DistributedCollection;
        }
    }


    private readonly object _collectionsLock = new();
    void ICommandReceiver.HandleCommand(CommandDTO command)
    {
        lock (_collectionsLock)
        {
            if (command.Action == ChangeType.CreateCollection)
            {
                var info = JsonSerializer.Deserialize<DictionaryTypeInfo>(command.JsonObject)
                    ?? throw new InvalidOperationException("Invalid collection type information.");
                if (info.CollectionType != "DistributedDictionary")
                    throw new InvalidOperationException($"Unsupported collection type: {info.CollectionType}");

                var keyType = Type.GetType(info.KeyType, throwOnError: true)!;
                var valueType = Type.GetType(info.ValueType, throwOnError: true)!;
                ValidateKeyType(keyType);

                if (_distributedCollections.TryGetValue(command.CollectionName, out var existing))
                {
                    var dictionaryType = typeof(IDictionary<,>).MakeGenericType(keyType, valueType);
                    if (!dictionaryType.IsInstanceOfType(existing.DistributedCollection))
                        throw new InvalidOperationException($"Collection '{command.CollectionName}' has a different type.");
                    return;
                }

                var factory = typeof(ReplicationSet).GetMethod(nameof(CreateDictionary), BindingFlags.Instance | BindingFlags.NonPublic)!
                    .MakeGenericMethod(keyType, valueType);
                var collection = (CollectionInfo)factory.Invoke(this, new object[] { command.CollectionName })!;
                _distributedCollections.Add(command.CollectionName, collection);
            }
            else if (_distributedCollections.TryGetValue(command.CollectionName, out var collection))
            {
                collection.Node.CommandCallback(command);
            }
            else
            {
                throw new InvalidOperationException($"Collection '{command.CollectionName}' does not exist.");
            }
        }
    }

    private CollectionInfo CreateDictionary<TKey, TValue>(string name) where TKey : notnull
    {
        var node = new DistributedCollectionNode<TKey, TValue>(name, _commandSender);
        var dictionary = new DistributedDictionary<TKey, TValue>(node);
        return new CollectionInfo("DistributedDictionary", dictionary, node);
    }

    private static void ValidateKeyType(Type keyType)
    {
        if (!keyType.IsPrimitive && !keyType.IsEnum && keyType != typeof(string) &&   // TODO: Senere skal vi vælge hvilke typer vi understøtter
            keyType != typeof(Guid) && keyType != typeof(decimal) && keyType != typeof(DateTime) &&
            keyType != typeof(DateTimeOffset) && keyType != typeof(TimeSpan) &&
            keyType != typeof(DateOnly) && keyType != typeof(TimeOnly))
        {
            throw new NotSupportedException($"Key type '{keyType}' is not supported. Use a non-nullable primitive, enum, string, Guid, or simple date/time type.");
        }
    }

    public (string Name, string CollectionType)[] GetAllCollections()
    {
        lock (_collectionsLock)
        {
            return _distributedCollections.Select(p => (p.Key, p.Value.CollectionType)).ToArray();
        }
    }
}