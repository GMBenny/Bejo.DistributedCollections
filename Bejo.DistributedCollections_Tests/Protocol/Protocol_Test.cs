using Bejo.DistributedCollections;
using Bejo.DistributedCollections.Network;
using Bejo.DistributedCollections.Contracts;
using FluentAssertions;
using System.Text;

namespace Bejo.DistributedCollections_Tests;

public class ProtocolTest
{
    [Fact]
    public async Task CreateProtocol_WithServer_NoException()
    {
        var protocolServer = new Protocol(new UnusedReceiver());

        var server = new Server(11001, false, null, protocolServer.MessageHandler);
        await TestHelpers.WaitForServerToStartAsync(server);

        var protocolClient = new Protocol("localhost", 11001, 3, new TimeSpan(0, 0, 2), false, null);
    }

    [Fact]
    public async Task CreateProtocol_WithoutServer_NoException()
    {
        var protocolClient = new Protocol("localhost", 11002, 3, new TimeSpan(0, 0, 2), false, null);
    }
    [Fact]
    public async Task SendPingAsync_WithoutServer_Offline()
    {
        var protocolClient = new Protocol("localhost", 11010, 3, new TimeSpan(0, 0, 2), false, null);

        var response = await protocolClient.SendPingAsync();

        response.LastStatus.Should().Be(NetworkStatus.Offline);
    }

    [Fact]
    public async Task SendPingAsync_WithOnlineServer_Online()
    {
        var protocolServer = new Protocol(new UnusedReceiver());

        var server = new Server(11003, false, null, protocolServer.MessageHandler);
        await TestHelpers.WaitForServerToStartAsync(server);

        var protocolClient = new Protocol("localhost", 11003, 3, new TimeSpan(0, 0, 2), false, null);
        
        var networkStat = await protocolClient.SendPingAsync();
               
        networkStat.LastStatus.Should().Be(NetworkStatus.Online);
        networkStat.RoundtripTime.Should().BeLessThan(TimeSpan.FromSeconds(2));
    }

    [Fact]
    public async Task SendPingAsync_WithoutHandshake_AuthenticationRequired()
    {
        var protocolServer = new Protocol(new UnusedReceiver(), "Server", "shared-key", () => ThisStatus.LoadingData);
        var server = new Server(11030, false, null, protocolServer.AuthenticatedMessageHandler);
        await TestHelpers.WaitForServerToStartAsync(server);

        var protocolClient = new Protocol("localhost", 11030, 3, TimeSpan.FromSeconds(2), false, null, "Client", "shared-key");

        var networkStat = await protocolClient.SendPingAsync();

        networkStat.LastStatus.Should().Be(NetworkStatus.UnexpectedResult);
    }

    [Fact]
    public async Task SendHelloAsync_WithMatchingKey_AllowsCommunication()
    {
        var protocolServer = new Protocol(new UnusedReceiver(), "Server", "shared-key", () => ThisStatus.LoadingData);
        var server = new Server(11031, false, null, protocolServer.AuthenticatedMessageHandler);
        await TestHelpers.WaitForServerToStartAsync(server);

        var protocolClient = new Protocol("localhost", 11031, 3, TimeSpan.FromSeconds(2), false, null, "Client", "shared-key");

        await protocolClient.SendHelloAsync();
        var networkStat = await protocolClient.SendPingAsync();

        networkStat.LastStatus.Should().Be(NetworkStatus.LoadingData);
    }

    [Fact]
    public async Task SendHelloAsync_WithMismatchingKey_ConnectionRejected()
    {
        var protocolServer = new Protocol(new UnusedReceiver(), "Server", "shared-key", () => ThisStatus.LoadingData);
        var server = new Server(11032, false, null, protocolServer.AuthenticatedMessageHandler);
        await TestHelpers.WaitForServerToStartAsync(server);

        var protocolClient = new Protocol("localhost", 11032, 3, TimeSpan.FromSeconds(2), false, null, "Client", "wrong-key");

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => protocolClient.SendHelloAsync());

        exception.Message.Should().Contain("Node key rejected");
    }

    [Fact]
    public async Task SendHelloAsync_ServerHasKeyClientHasNoKey_ConnectionRejected()
    {
        var protocolServer = new Protocol(new UnusedReceiver(), "Server", "shared-key", () => ThisStatus.LoadingData);
        var server = new Server(11033, false, null, protocolServer.AuthenticatedMessageHandler);
        await TestHelpers.WaitForServerToStartAsync(server);

        var protocolClient = new Protocol("localhost", 11033, 3, TimeSpan.FromSeconds(2), false, null, "Client", null);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => protocolClient.SendHelloAsync());

        exception.Message.Should().Contain("Node key rejected");
    }

    [Fact]
    public async Task SendHelloAsync_ServerHasNoKeyClientHasKey_ConnectionRejected()
    {
        var protocolServer = new Protocol(new UnusedReceiver(), "Server", null, () => ThisStatus.LoadingData);
        var server = new Server(11034, false, null, protocolServer.AuthenticatedMessageHandler);
        await TestHelpers.WaitForServerToStartAsync(server);

        var protocolClient = new Protocol("localhost", 11034, 3, TimeSpan.FromSeconds(2), false, null, "Client", "any-key");

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => protocolClient.SendHelloAsync());

        exception.Message.Should().Contain("Node key rejected");
    }

    [Fact]
    public async Task SendHelloAsync_BothNodesHaveNoKey_AllowsCommunication()
    {
        var protocolServer = new Protocol(new UnusedReceiver(), "Server", null, () => ThisStatus.LoadingData);
        var server = new Server(11035, false, null, protocolServer.AuthenticatedMessageHandler);
        await TestHelpers.WaitForServerToStartAsync(server);

        var protocolClient = new Protocol("localhost", 11035, 3, TimeSpan.FromSeconds(2), false, null, "Client", null);

        await protocolClient.SendHelloAsync();
        var networkStat = await protocolClient.SendPingAsync();

        networkStat.LastStatus.Should().Be(NetworkStatus.LoadingData);
    }

    [Fact]
    public async Task SendSystemMessage_WithoutCompressionOrEncryption_ReturnsMatchingHash()
    {
        var protocolServer = new Protocol(new UnusedReceiver());

        var server = new Server(11004, false, null, protocolServer.MessageHandler);
        await TestHelpers.WaitForServerToStartAsync(server);

        var protocolClient = new Protocol("localhost", 11004, 3, new TimeSpan(0, 0, 2), false, null);

        var message = "Der var engang i lille gris";
        
        var response = await protocolClient.SendSystemMessage(message);

        response.Should().Be(DistributedCollections.Helpers.Helper.Hash(Encoding.UTF8.GetBytes(message)).ToString());
    }

    [Fact]
    public async Task SendSystemMessage_WithCompression_ReturnsMatchingHash()
    {
        var protocolServer = new Protocol(new UnusedReceiver());

        var server = new Server(11005, true, null, protocolServer.MessageHandler);
        await TestHelpers.WaitForServerToStartAsync(server);

        var protocolClient = new Protocol("localhost", 11005, 3, new TimeSpan(0, 0, 2),true, null);

        var message = "Der var engang i lille gris";
        
        var response = await protocolClient.SendSystemMessage(message);

        var debug = Encoding.UTF8.GetBytes(message);
        var debug2 = DistributedCollections.Helpers.Helper.Hash(Encoding.UTF8.GetBytes(message));
        response.Should().Be(DistributedCollections.Helpers.Helper.Hash(Encoding.UTF8.GetBytes(message)).ToString());
    }

    [Fact]
    public async Task SendSystemMessage_ServerCompressedClientUncompressed_ThrowsException()
    {
        var protocolServer = new Protocol(new UnusedReceiver());

        var server = new Server(11007, true, null, protocolServer.MessageHandler);
        await TestHelpers.WaitForServerToStartAsync(server);

        var protocolClient = new Protocol("localhost", 11007, 3, new TimeSpan(0, 0, 2),false, null);

        var message = "Der var engang i lille gris";

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => protocolClient.SendSystemMessage(message));
        
        exception.Message.Should().Contain("Failed to decompress message.");
    }
    [Fact]
    public async Task SendSystemMessage_ClientCompressedServerUncompressed_ThrowsException()
    {
        var protocolServer = new Protocol(new UnusedReceiver());
        
        var server = new Server(11008, false, null, protocolServer.MessageHandler);
        await TestHelpers.WaitForServerToStartAsync(server);

        var protocolClient = new Protocol("localhost", 11008, 3, new TimeSpan(0, 0, 2),true, null);

        var message = "Der var engang i lille gris";

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => protocolClient.SendSystemMessage(message));

        exception.Message.Should().StartWith("Error from server:");
        exception.Message.Should().Contain("Error: Unrecognized message format."); // There is an error, but it doesn't know that it is compression, so it will just say unrecognized message format.
    }

    [Fact]
    public async Task SendSystemMessage_WithEncryption_ReturnsMatchingHash()
    {
        var protocolServer = new Protocol(new UnusedReceiver());

        var server = new Server(11009, false, ["abc123abc"], protocolServer.MessageHandler);
        await TestHelpers.WaitForServerToStartAsync(server);

        var protocolClient = new Protocol("localhost", 11009, 3, new TimeSpan(0, 0, 2),false, "abc123abc");

        var message = "Der var engang i lille gris";
        
        var response = await protocolClient.SendSystemMessage(message);

        response.Should().Be(DistributedCollections.Helpers.Helper.Hash(Encoding.UTF8.GetBytes(message)).ToString());
    }
    [Fact]
    public async Task SendSystemMessage_WithSecondaryEncryptionKey_ReturnsMatchingHash()
    {
        var protocolServer = new Protocol(new UnusedReceiver());

        var server = new Server(11011, false, ["985jdgfwjr3", "abc123abc"], protocolServer.MessageHandler);
        await TestHelpers.WaitForServerToStartAsync(server);

        var protocolClient = new Protocol("localhost", 11011, 3, new TimeSpan(0, 0, 2),false, "abc123abc");

        var message = "Der var engang i lille gris";
        
        var response = await protocolClient.SendSystemMessage(message);

        response.Should().Be(DistributedCollections.Helpers.Helper.Hash(Encoding.UTF8.GetBytes(message)).ToString());
    }
    [Fact]
    public async Task SendSystemMessage_WithWrongEncryptionKey_ThrowsException()
    {
        var protocolServer = new Protocol(new UnusedReceiver());

        var server = new Server(11020, false, ["abc123abc"], protocolServer.MessageHandler);
        await TestHelpers.WaitForServerToStartAsync(server);

        var protocolClient = new Protocol("localhost", 11020, 3, new TimeSpan(0, 0, 2),false, "123aba321");

        var message = "Der var engang i lille gris";
        
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () => await protocolClient.SendSystemMessage(message));
        exception.Message.Should().StartWith("Error from server:");
        exception.Message.Should().Contain("Failed to decrypt message.");
    }


    private sealed class UnusedReceiver : ICommandReceiver
    {
        public void HandleCommand(CommandDTO command)
        { }
    }
}