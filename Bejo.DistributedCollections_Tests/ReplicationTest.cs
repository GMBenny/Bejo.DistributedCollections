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
    public async Task RemoteDictionaryReceivesChangesWithoutSendingThemBack()
    {
        var sourceSender = new RecordingSender();
        var targetSender = new RecordingSender();
        var source = new ReplicationSet(sourceSender);
        var target = new ReplicationSet(targetSender);
        var sourceDictionary = source.GetOrCreateDistributedDictionary<string, int>("numbers");

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
            var targetDictionary = target.GetOrCreateDistributedDictionary<string, int>("numbers");
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
        var firstDictionary = first.GetOrCreateDistributedDictionary<string, int>("shared");
        firstDictionary.Add("first", 1);

        await WaitUntilAsync(() => second.GetAllCollections().Any(c => c.Name == "shared"));
        var secondDictionary = second.GetOrCreateDistributedDictionary<string, int>("shared");
        await WaitUntilAsync(() => secondDictionary.TryGetValue("first", out var value) && value == 1);

        secondDictionary.Add("second", 2);
        await WaitUntilAsync(() => firstDictionary.TryGetValue("second", out var value) && value == 2);
    }

    [Fact]
    public void NullValuesRemainPresentInTheDictionary()
    {
        var set = new ReplicationSet(new RecordingSender());
        var dictionary = set.GetOrCreateDistributedDictionary<string, string?>("nullable");

        dictionary.Add("key", null);

        Assert.True(dictionary.TryGetValue("key", out var value));
        Assert.Null(value);
        Assert.False(dictionary.TryGetValue("missing", out _));
    }

    [Fact]
    public void RepeatedCreateIsIdempotentButConflictingTypeIsRejected()
    {
        var sender = new RecordingSender();
        var source = new ReplicationSet(sender);
        var targetSender = new RecordingSender();
        var target = new ReplicationSet(targetSender);
        source.GetOrCreateDistributedDictionary<Guid, string>("items");
        var create = sender.Commands[0];
        var protocol = new Protocol((ICommandReceiver)target);

        Assert.False(Deliver(protocol, create).StartsWith("Error:"));
        Assert.False(Deliver(protocol, create).StartsWith("Error:"));
        Assert.Single(target.GetAllCollections());
        Assert.Empty(targetSender.Commands);

        var incompatibleInfo = new DictionaryTypeInfo("DistributedDictionary", typeof(string).AssemblyQualifiedName!, typeof(string).AssemblyQualifiedName!);
        var incompatible = new CommandDTO("items", JsonSerializer.Serialize(incompatibleInfo), ChangeType.CreateCollection, 0, 0);
        Assert.StartsWith("Error:", Deliver(protocol, incompatible));
        Assert.IsAssignableFrom<IDictionary<Guid, string>>(target.GetOrCreateDistributedDictionary<Guid, string>("items"));
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
