using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using Bejo.DistributedCollections.Contracts;

namespace Bejo.DistributedCollections.Dictionary;

internal class DistributedCollectionNode<TKey, TValue>(string name, ICommandSender commandSender, ReplicationSet.HandleConflict<TKey, TValue> handleConflict) : IDistributedCollectionNode where TKey : notnull
{
    private readonly ConcurrentDictionary<TKey, LocalStorage<TKey, TValue>> _dictionaryReplica = new();
    private ReplicationSet.HandleConflict<TKey, TValue> _handleConflict = handleConflict;

    internal ICollection<TKey> Keys => _dictionaryReplica.Keys;

    internal ICollection<TValue> Values => _dictionaryReplica.Values.Select(v => v.Value).ToList();

    internal void SetConflictHandler(ReplicationSet.HandleConflict<TKey, TValue> handleConflict)
    {
        _handleConflict = handleConflict;
    }

    internal bool TryCreateUpdate(TKey key, TValue value)
    {
        var localValue = new LocalStorage<TKey, TValue>(key, value);

        // If the key already exists, we update it and send a replication command
        if (_dictionaryReplica.TryGetValue(key, out var existingValue))
        {
            commandSender.SendCommand(new CommandDTO(name, localValue.JsonString, ChangeType.AddUpdated, existingValue.ValueHash, localValue.ValueHash));

            _dictionaryReplica[key] = localValue; // Update the value in the local dictionary

            return true;
        }
        else // If the key does not exist, we create it and send a replication command
        {
            var createResult = _dictionaryReplica.TryAdd(key, localValue); // Attempt to add the new key-value pair

            // If local change was a success, then replicate the change.
            if (createResult)
                commandSender.SendCommand(new CommandDTO(name, localValue.JsonString, ChangeType.AddUpdated, 0, localValue.ValueHash));
            
            return createResult;
        }
    }

    internal bool TryGetValue(TKey key, [MaybeNullWhen(false)] out TValue value)
    {
        if (_dictionaryReplica.TryGetValue(key, out var localStorage))
        {
            value = localStorage.Value;
            return true;
        }

        value = default;
        return false;
    }

    internal bool Remove(TKey key)
    {
        var removeResult = _dictionaryReplica.TryRemove(key, out var removedValue);

        // If local change was a success, then replicate the change.
        if (removeResult && removedValue != null)
            commandSender.SendCommand(new CommandDTO(name, removedValue.JsonString, ChangeType.Removed, removedValue.ValueHash, 0));

        return removeResult;
    }

    internal void Clear()
    {
        _dictionaryReplica.Clear();

        commandSender.SendCommand(new CommandDTO(name, string.Empty, ChangeType.ClearCollection, 0, 0));
    }



    public void CommandCallback(CommandDTO command)
    {
        switch (command.Action)
        {
            case ChangeType.AddUpdated:
                var l = new LocalStorage<TKey, TValue>(command.JsonObject);
                
                if (_dictionaryReplica.TryGetValue(l.Key, out var existingValue))
                {
                    if (existingValue.ValueHash == command.FromValueHash)
                    {
                        _dictionaryReplica[l.Key] = l;
                        // TODO: Fyr event af, så brugeren ved dette objekt er blevet opdateret via netwærket.
                    }
                    else
                    {
                        // Der er sket en konflikt - håndter det - Kald brugeren, som skal fixe det
                        Console.WriteLine($"Conflict detected for key {l.Key}. Existing value hash: {existingValue.ValueHash}, incoming value hash: {command.FromValueHash}");
                        var resolvedValue = _handleConflict(l.Key, existingValue.Value, l.Value);
                        _dictionaryReplica[l.Key] = new LocalStorage<TKey, TValue>(l.Key, resolvedValue);
                    }
                }
                else
                {
                    _dictionaryReplica.TryAdd(l.Key, l);
                    // TODO: Fyr event af, så brugeren ved dette objekt er blevet opdateret via netwærket.
                }

                break;
            case ChangeType.Removed:
                var removed = new LocalStorage<TKey, TValue>(command.JsonObject);
                _dictionaryReplica.TryRemove(removed.Key, out _);
                // TODO: Fyr event af, så brugeren ved dette objekt er blevet slettet via netwærket.
                break;
            case ChangeType.ClearCollection:
                _dictionaryReplica.Clear();
                // TODO: Fyr event af, så brugeren ved hele collectionen er blevet ryddet via netwærket.
                break;
            default:
                throw new InvalidOperationException($"Unsupported command action: {command.Action}");
        }
    }
}