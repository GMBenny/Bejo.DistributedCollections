# Bejo.DistributedCollections

**A lightweight distributed key-value collection library for .NET, built from scratch.**

Bejo.DistributedCollections is a personal C# project exploring distributed data storage, node-to-node communication, data replication, and conflict resolution.

The goal is to provide a familiar dictionary-like programming interface while keeping multiple instances of a collection synchronized across a predefined set of nodes.

The project is built from scratch as a learning and portfolio project, with a particular focus on understanding the challenges involved in building distributed systems.

## Features

- **Distributed dictionaries** – Work with key-value collections replicated across multiple nodes.
- **Automatic replication** – Changes made on one node are propagated to the other nodes in the replication set.
- **Conflict resolution** – Custom conflict resolution logic allows applications to decide how concurrent changes should be handled.
- **Multiple nodes** – A replication set can consist of multiple independently running instances.
- **TCP communication** – Nodes communicate directly over the network.
- **Familiar API** – Dictionary operations such as `TryAdd`, indexer access, `Remove`, and enumeration.
- **Configurable replication** – Configure node endpoints, connection timeouts, and replication settings.

## Architecture

A `ReplicationSet` represents a group of nodes that share replicated collections.

Each node runs its own instance of a `ReplicationSet`. Nodes know about the other members of the replication set and communicate directly with them.

A distributed dictionary is created through the replication set. Collections with the same name on different nodes represent the same logical distributed collection.

Conceptually, the architecture looks like this:

```text
             Replication Set
             /      |      \
            /       |       \
       Node 1    Node 2    Node 3
          |         |         |
       Dict A    Dict A    Dict A
          |         |         |
          +---------+---------+
             Replication
```

Each node maintains its own local copy of the dictionary. Changes are propagated between nodes to keep the copies synchronized.

## Getting started

### 1. Create a replication set

Create a `ReplicationSet` instance for each node.

Example configuration for a three-node setup:

```csharp
var rs1 = new Bejo.DistributedCollections.ReplicationSet(
    new Bejo.DistributedCollections.ReplicationSetSettings(
        "rs1", // nodeName
        10001, // listenerPort
        true,  // use encryption (true/false)
        ["abc123abc"],  // encryption key (null/[keys]) - Uses first key for encryption and all keys left to right for decryption - Made for rolling updates of keys
        [
            new("localhost", 10002, TimeSpan.FromSeconds(2)), // Two other nodes are found on localhost:10002 and 10003. rs1 will ping 10002 every 2 secounds
            new("localhost", 10003, TimeSpan.FromSeconds(2))
        ],
        3, // Retry counts
        TimeSpan.FromSeconds(2)  // timeout
    ));
```

The other nodes are configured similarly, using their own identifiers and ports.

### 2. Create a distributed dictionary

Create or retrieve a distributed dictionary by name:

```csharp
var dictionary = rs1.GetOrCreateDistributedDictionary<int, string>(
    "myDictionary",  // Name the dictionary
    HandleConflict); // delegate to handle update conflicts
```

The generic parameters define the key and value types.

The conflict handler determines how conflicting values are resolved.

### 3. Work with the dictionary

The API is designed to feel familiar to developers used to working with dictionaries:

```csharp
dictionary.TryAdd(1, "one");

dictionary[1] = "ONE";

if (dictionary.ContainsKey(1))
{
    Console.WriteLine(dictionary[1]);
}

dictionary.Remove(1);
```

Changes are replicated to the other nodes in the replication set.

## Conflict resolution

One of the interesting challenges in distributed systems is handling concurrent writes.

For example, imagine two nodes adding the same key before either node has received the other's change:

```csharp
dict1.TryAdd(1, "one");
dict2.TryAdd(1, "ONE");
```

Both nodes have independently modified their local dictionaries.

When the conflicting changes are replicated, the application-provided conflict handler determines the resulting value.

Example:

```csharp
private string HandleConflict(
    int key,
    ConflictData<string> a,
    ConflictData<string> b)
{
    return string.Compare(
        a.Value,
        b.Value,
        StringComparison.Ordinal) < 0
            ? a.Value
            : b.Value;
}
```

In this example, the lexicographically greatest value wins.

This is only an example. Applications can implement their own conflict resolution strategies depending on their requirements.

## Testing

The project includes automated tests using xUnit and FluentAssertions.

The tests exercise multiple independently configured replication sets communicating over TCP.

Current test scenarios include:

| Test | Description |
|---|---|
| `TryAdd_Sunshine_GetData` | Verify that an added value is replicated to another node. |
| `Remove_Sunshine_GetData` | Verify that removing an entry is replicated. |
| `Update_Sunshine_GetData` | Verify that updating an entry is replicated. |
| `Update_MissingKey_Error` | Verify expected dictionary behavior when accessing a missing key. |
| `HandleConflict_Conflict_SameData` | Verify conflict resolution when two nodes add different values for the same key. |

These tests demonstrate basic replication and conflict resolution across multiple nodes.

## Technology

- C#
- .NET
- TCP/IP networking
- Asynchronous programming
- xUnit
- FluentAssertions

The implementation intentionally avoids relying on existing distributed collection frameworks. The objective is to understand and implement the underlying mechanisms rather than wrapping an existing solution.

## Project status

**Work in progress.**

This is an experimental project and is not intended for production use yet.

The basic replication scenarios are implemented and tested, but distributed systems introduce many challenges beyond the happy path.

Areas that require further development and testing include:

- Network interruptions and automatic recovery.
- Node disconnections and reconnections.
- Reliable delivery and message acknowledgements.
- Synchronization of nodes that have been offline.
- Concurrent writes and more complex conflict scenarios.
- Persistence and recovery after process restarts.
- Consistency guarantees and failure handling.

The project will evolve as these challenges are explored and implemented.

## Motivation

This project started as a way to explore distributed systems in greater depth.

Rather than relying on an existing framework, I wanted to understand how multiple independent nodes can coordinate changes, exchange data, handle conflicts, and recover from communication failures.

It is also an opportunity to explore software architecture, network programming, concurrency, and API design in a practical C# project.
