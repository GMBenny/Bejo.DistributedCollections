using Bejo.DistributedCollections.Network;
using Bejo.DistributedCollections.Contracts;
using FluentAssertions;
using System.Text;

namespace Bejo.DistributedCollections_Tests;

public class ProtocolTest
{
    [Fact]
    public async Task CreateProtocol_Sunshine_NoException()
    {
        var protocolServer = new Protocol(new UnusedReceiver());

        var server = new Server(11001, false, null, protocolServer.MessageHandler);
        await TestHelpers.WaitForServerToStartAsync(server);

        var protocolClient = new Protocol("localhost", 11001, 3, new TimeSpan(0, 0, 2), false, null);
    }

    [Fact]
    public async Task CreateProtocol_NoServer_NoResponse()
    {
        var protocolClient = new Protocol("localhost", 11002, 3, new TimeSpan(0, 0, 2), false, null);
    }
    [Fact]
    public async Task SendPing_NoServer_Exception()
    {
        var protocolClient = new Protocol("localhost", 11010, 3, new TimeSpan(0, 0, 2), false, null);

        var response = await protocolClient.SendPingAsync();

        response.LastStatus.Should().Be(NetworkStatus.Offline);
    }

    [Fact]
    public async Task SendPing_Sunshine_Online()
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
    public async Task SendSystemMessage_Sunshine_OkHash()
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
    public async Task SendSystemMessage_Compression_OkHash()
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
    public async Task SendSystemMessage_MismatchedCompressionA_Exception()
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
    public async Task SendSystemMessage_MismatchedCompressionB_Exception()
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
    public async Task SendSystemMessage_Encryption_OkHash()
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
    public async Task SendSystemMessage_EncryptionSecondaryPassword_OkHash()
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
    public async Task SendSystemMessage_EncryptionWrongPassword_Exception()
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