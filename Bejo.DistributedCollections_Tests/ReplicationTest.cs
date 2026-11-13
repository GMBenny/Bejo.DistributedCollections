using Bejo.DistributedCollections;
using Bejo.DistributedCollections.Contracts;
using Bejo.DistributedCollections.Network;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;

namespace Bejo.DistributedCollections_Tests;

public class ReplicationTest
{
    private sealed class RecordingSender : ICommandSender
    {
        public List<CommandDTO> Commands { get; } = new();

        public void SendCommand(CommandDTO command) => Commands.Add(command);
    }

    [Fact]
    public void LocalStatusIsStartedEvenWhenPeerHasNotResponded()
    {
        var set = new ReplicationSet(Settings(0, 1));

        var status = set.GetReplicationSetStatus();

        Assert.Equal(ThisStatus.Started, status.ThisStatus);
        Assert.Equal(NetworkStatus.Offline, Assert.Single(status.NetworkStats).LastStatus);
    }

    [Fact]
    public void LocalStatusIsFaultedWhenListenerCannotStart()
    {
        var reservation = new TcpListener(IPAddress.IPv6Any, 0);
        reservation.Server.DualMode = true;
        reservation.Server.ExclusiveAddressUse = true;
        reservation.Start();
        try
        {
            var port = ((IPEndPoint)reservation.LocalEndpoint).Port;
            var set = new ReplicationSet(new ReplicationSetSettings("test", port, false, null, false, [], 1, TimeSpan.FromSeconds(1)));

            Assert.Equal(ThisStatus.Faulted, set.GetReplicationSetStatus().ThisStatus);
            Assert.Empty(set.GetReplicationSetStatus().NetworkStats);
        }
        finally
        {
            reservation.Stop();
        }
    }

    [Fact]
    public async Task ServerReturnsToStoppedAfterCancellation()
    {
        var server = new Server(0, false, null, message => message);
        using var cancellation = new CancellationTokenSource();
        Assert.Equal(Server.ServerState.Stopped, server.State);

        var serverTask = server.StartAsync(cancellation.Token);
        Assert.Equal(Server.ServerState.started, server.State);
        cancellation.Cancel();
        await serverTask.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(Server.ServerState.Stopped, server.State);
        Assert.Equal(ThisStatus.Stopped, new ReplicationSet(new RecordingSender()).GetReplicationSetStatus().ThisStatus);
    }

    [Fact]
    public async Task RemoteDictionaryReceivesChangesWithoutSendingThemBack()
    {
        var sourceSender = new RecordingSender();
        var targetSender = new RecordingSender();
        var source = new ReplicationSet(sourceSender);
        var target = new ReplicationSet(targetSender);
        var sourceDictionary = source.GetOrCreateDistributedDictionary<string, int>("numbers", (key, local, incoming) => local);

        var reservation = new TcpListener(IPAddress.Loopback, 0);
        reservation.Start();
        var port = ((IPEndPoint)reservation.LocalEndpoint).Port;
        reservation.Stop();

        using var cancellation = new CancellationTokenSource();
        var protocol = new Protocol((ICommandReceiver)target);
        var server = new Server(port, false, null, protocol.MessageHandler);
        var serverTask = server.StartAsync(cancellation.Token);

        try
        {
            var client = new Protocol("127.0.0.1", port, 1, TimeSpan.FromSeconds(2), false, null);
            await client.SendCommand(sourceSender.Commands[0]);
            var targetDictionary = target.GetOrCreateDistributedDictionary<string, int>("numbers", (key, local, incoming) => local);
            Assert.Empty(targetSender.Commands);

            sourceDictionary.Add("one", 1);
            await client.SendCommand(sourceSender.Commands[^1]);
            Assert.Equal(1, targetDictionary["one"]);

            sourceDictionary["one"] = 2;
            await client.SendCommand(sourceSender.Commands[^1]);
            Assert.Equal(2, targetDictionary["one"]);

            sourceDictionary.Remove("one");
            await client.SendCommand(sourceSender.Commands[^1]);
            Assert.False(targetDictionary.ContainsKey("one"));

            sourceDictionary.Add("two", 2);
            await client.SendCommand(sourceSender.Commands[^1]);
            sourceDictionary.Clear();
            await client.SendCommand(sourceSender.Commands[^1]);
            Assert.Empty(targetDictionary);
            Assert.Empty(targetSender.Commands);
        }
        finally
        {
            cancellation.Cancel();
            await serverTask.WaitAsync(TimeSpan.FromSeconds(5));
        }
    }

    [Fact]
    public async Task ReplicationSetsExchangeChangesThroughTheirNetworkManagers()
    {
        var firstReservation = new TcpListener(IPAddress.Loopback, 0);
        var secondReservation = new TcpListener(IPAddress.Loopback, 0);
        firstReservation.Start();
        secondReservation.Start();
        var firstPort = ((IPEndPoint)firstReservation.LocalEndpoint).Port;
        var secondPort = ((IPEndPoint)secondReservation.LocalEndpoint).Port;
        firstReservation.Stop();
        secondReservation.Stop();

        var first = new ReplicationSet(Settings(firstPort, secondPort));
        var second = new ReplicationSet(Settings(secondPort, firstPort));
        var firstDictionary = first.GetOrCreateDistributedDictionary<string, int>("shared", (key, local, incoming) => local);
        firstDictionary.Add("first", 1);

        await WaitUntilAsync(() => second.GetAllCollections().Any(c => c.Name == "shared"));
        var secondDictionary = second.GetOrCreateDistributedDictionary<string, int>("shared", (key, local, incoming) => local);
        await WaitUntilAsync(() => secondDictionary.TryGetValue("first", out var value) && value == 1);

        secondDictionary.Add("second", 2);
        await WaitUntilAsync(() => firstDictionary.TryGetValue("second", out var value) && value == 2);
    }

    [Fact]
    public void NullValuesRemainPresentInTheDictionary()
    {
        var set = new ReplicationSet(new RecordingSender());
        var dictionary = set.GetOrCreateDistributedDictionary<string, string?>("nullable", (key, local, incoming) => local);

        dictionary.Add("key", null);

        Assert.True(dictionary.TryGetValue("key", out var value));
        Assert.Null(value);
        Assert.False(dictionary.TryGetValue("missing", out _));
    }

    [Fact]
    public void RemoteDictionaryUsesDefaultThenReplacesConflictHandler()
    {
        var sourceSender = new RecordingSender();
        var targetSender = new RecordingSender();
        var source = new ReplicationSet(sourceSender);
        var target = new ReplicationSet(targetSender);
        var sourceDictionary = source.GetOrCreateDistributedDictionary<string, int>("items", (key, local, incoming) => local);
        var protocol = new Protocol((ICommandReceiver)target);

        Assert.False(Deliver(protocol, sourceSender.Commands[0]).StartsWith("Error:"));
        sourceDictionary.Add("key", 1);
        var initial = sourceSender.Commands[^1];
        Assert.False(Deliver(protocol, initial).StartsWith("Error:"));

        sourceDictionary["key"] = 2;
        var update = sourceSender.Commands[^1];
        var conflict = new CommandDTO("items", update.JsonObject, update.Action, initial.ToValueHash ^ 1, update.ToValueHash);
        Assert.False(Deliver(protocol, conflict).StartsWith("Error:"));

        var calls = 0;
        var targetDictionary = target.GetOrCreateDistributedDictionary<string, int>("items", (key, local, incoming) =>
        {
            calls++;
            Assert.Equal("key", key);
            Assert.Equal(1, local);
            Assert.Equal(2, incoming);
            return incoming;
        });
        Assert.Equal(1, targetDictionary["key"]);

        Assert.False(Deliver(protocol, conflict).StartsWith("Error:"));
        Assert.Equal(1, calls);
        Assert.Equal(2, targetDictionary["key"]);

        var sameDictionary = target.GetOrCreateDistributedDictionary<string, int>("items", (key, local, incoming) => local + incoming);
        Assert.Same(targetDictionary, sameDictionary);
        Assert.False(Deliver(protocol, conflict).StartsWith("Error:"));
        Assert.Equal(4, targetDictionary["key"]);
        Assert.Equal(1, calls);
        Assert.Empty(targetSender.Commands);
    }

    [Fact]
    public void RepeatedCreateIsIdempotentButConflictingTypeIsRejected()
    {
        var sender = new RecordingSender();
        var source = new ReplicationSet(sender);
        var targetSender = new RecordingSender();
        var target = new ReplicationSet(targetSender);
        source.GetOrCreateDistributedDictionary<Guid, string>("items", (key, local, incoming) => local);
        var create = sender.Commands[0];
        var protocol = new Protocol((ICommandReceiver)target);

        Assert.False(Deliver(protocol, create).StartsWith("Error:"));
        Assert.False(Deliver(protocol, create).StartsWith("Error:"));
        Assert.Single(target.GetAllCollections());
        Assert.Empty(targetSender.Commands);

        var incompatibleInfo = new DictionaryTypeInfo("DistributedDictionary", typeof(string).AssemblyQualifiedName!, typeof(string).AssemblyQualifiedName!);
        var incompatible = new CommandDTO("items", JsonSerializer.Serialize(incompatibleInfo), ChangeType.CreateCollection, 0, 0);
        Assert.StartsWith("Error:", Deliver(protocol, incompatible));
        Assert.IsAssignableFrom<IDictionary<Guid, string>>(target.GetOrCreateDistributedDictionary<Guid, string>("items", (key, local, incoming) => local));
    }

    [Fact]
    public void UnknownCollectionIsNotAcknowledged()
    {
        var target = new ReplicationSet(new RecordingSender());
        var protocol = new Protocol((ICommandReceiver)target);
        var command = new CommandDTO("missing", "", ChangeType.ClearCollection, 0, 0);

        Assert.StartsWith("Error:", Deliver(protocol, command));
    }

    private static string Deliver(Protocol protocol, CommandDTO command)
    {
        var json = JsonSerializer.Serialize(command);
        var hash = Bejo.DistributedCollections.Helpers.Helper.Hash(Encoding.UTF8.GetBytes(json));
        var message = Encoding.UTF8.GetBytes($"COLLECTION:COMMAND:{hash}:{json}");
        return Encoding.UTF8.GetString(protocol.MessageHandler(message));
    }

    private static ReplicationSetSettings Settings(int port, int peerPort) =>
        new("test", port, false, null, false,
            [new NodeSettings("127.0.0.1", peerPort, TimeSpan.FromMinutes(1), false)],
            1, TimeSpan.FromSeconds(1));

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (!condition() && DateTime.UtcNow < deadline)
            await Task.Delay(20);

        Assert.True(condition(), "The expected replicated change did not arrive.");
    }
}
