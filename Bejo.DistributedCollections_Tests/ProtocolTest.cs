using Bejo.DistributedCollections.Network;
using FluentAssertions;
using System.Text;

namespace Bejo.DistributedCollections_Tests;

public class ProtocolTest
{
    [Fact]
    public async Task Sunshine()
    {
        var protocolServer = new Protocol();

        var server = new Server(10002, true, new List<string>{ "abc123abc", "123454321"}, protocolServer.MessageHandler);
        Task.Run(async () => { await server.StartAsync(); });

        do
        {
            await Task.Delay(25);
        } while (server.State != Server.ServerState.started);

        var protocolClient = new Protocol("localhost", 10002, 3, new TimeSpan(0, 0, 2), true, "123454321");
    }

    [Fact]
    public async Task SendPing()
    {
        var protocolServer = new Protocol();
        var server = new Server(10003, true, new List<string>{ "abc123abc", "123454321"}, protocolServer.MessageHandler);
        Task.Run(async () => { await server.StartAsync(); });

        do
        {
            await Task.Delay(25);
        } while (server.State != Server.ServerState.started);

        var protocolClient = new Protocol("localhost", 10003, 3, new TimeSpan(0, 0, 2), true, "123454321");
        var networkStat = await protocolClient.SendPing();
               
        networkStat.LastStatus.Should().Be(NetworkStatus.Online);
        networkStat.RoundtripTime.Should().BeLessThan(TimeSpan.FromSeconds(2));
    }

    [Fact]
    public async Task ReverseMessageWithHashCheck()
    {
        var protocolServer = new Protocol();
        var server = new Server(10004, true, new List<string>{ "abc123abc", "123454321"}, protocolServer.MessageHandler);
        Task.Run(async () => { await server.StartAsync(); });

        do
        {
            await Task.Delay(25);
        } while (server.State != Server.ServerState.started);

        var protocolClient = new Protocol("localhost", 10004, 3, new TimeSpan(0, 0, 2),true, "123454321");

        var message = "Der var engang i lille gris";
        
        var response = await protocolClient.SendSystemMessage(message);

        response.Should().Be(Bejo.DistributedCollections.Helpers.Helper.Hash(Encoding.UTF8.GetBytes(message)).ToString());
    }

    [Fact]
    public async Task CompressionTest()
    {
        var protocolServer = new Protocol();
        var server = new Server(10005, true, null, protocolServer.MessageHandler);
        Task.Run(async () => { await server.StartAsync(); });

        do
        {
            await Task.Delay(25);
        } while (server.State != Server.ServerState.started);

        var protocolClient = new Protocol("localhost", 10005, 3, new TimeSpan(0, 0, 2),true, null);

        var message = "Der var engang i lille gris";
        
        var response = await protocolClient.SendSystemMessage(message);

        response.Should().Be(Bejo.DistributedCollections.Helpers.Helper.Hash(Encoding.UTF8.GetBytes(message)).ToString());
    }
        [Fact]
    public async Task NoCompressionTest()
    {
        var protocolServer = new Protocol();
        var server = new Server(10006, false, null, protocolServer.MessageHandler);
        Task.Run(async () => { await server.StartAsync(); });

        do
        {
            await Task.Delay(25);
        } while (server.State != Server.ServerState.started);

        var protocolClient = new Protocol("localhost", 10006, 3, new TimeSpan(0, 0, 2),false, null);

        var message = "Der var engang i lille gris";
        
        var response = await protocolClient.SendSystemMessage(message);

        response.Should().Be(Bejo.DistributedCollections.Helpers.Helper.Hash(Encoding.UTF8.GetBytes(message)).ToString());
    }

    [Fact]
    public async Task MismatchedCompressionTestA()
    {
        var protocolServer = new Protocol();
        var server = new Server(10007, true, null, protocolServer.MessageHandler);
        Task.Run(async () => { await server.StartAsync(); });

        do
        {
            await Task.Delay(25);
        } while (server.State != Server.ServerState.started);

        var protocolClient = new Protocol("localhost", 10007, 3, new TimeSpan(0, 0, 2),false, null);

        var message = "Der var engang i lille gris";

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => protocolClient.SendSystemMessage(message));
        
        exception.Message.Should().Contain("Failed to decompress message.");
    }
    [Fact]
    public async Task MismatchedCompressionTestB()
    {
        var protocolServer = new Protocol();
        var server = new Server(10008, false, null, protocolServer.MessageHandler);
        Task.Run(async () => { await server.StartAsync(); });

        do
        {
            await Task.Delay(25);
        } while (server.State != Server.ServerState.started);

        var protocolClient = new Protocol("localhost", 10008, 3, new TimeSpan(0, 0, 2),true, null);

        var message = "Der var engang i lille gris";

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => protocolClient.SendSystemMessage(message));

        exception.Message.Should().StartWith("Error from server:");

        // Hvis ikke vi beder serveren om at dekomprimere, så opdager vi ikke at data er komprimeret og derfor får vi denne fejl
        exception.Message.Should().Contain("Error: Unrecognized message format.");
    }

    [Fact]
    public async Task EncryptionTest()
    {
        var protocolServer = new Protocol();
        var server = new Server(10009, false, new List<string>{"abc123abc"}, protocolServer.MessageHandler);
        Task.Run(async () => { await server.StartAsync(); });

        do
        {
            await Task.Delay(25);
        } while (server.State != Server.ServerState.started);

        var protocolClient = new Protocol("localhost", 10009, 3, new TimeSpan(0, 0, 2),false, "abc123abc");

        var message = "Der var engang i lille gris";
        
        var response = await protocolClient.SendSystemMessage(message);

        response.Should().Be(Bejo.DistributedCollections.Helpers.Helper.Hash(Encoding.UTF8.GetBytes(message)).ToString());
    }
    [Fact]
    public async Task EncryptionTest_SecondaryPassword()
    {
        var protocolServer = new Protocol();
        var server = new Server(10011, false, new List<string>{"985jdgfwjr3", "abc123abc"}, protocolServer.MessageHandler);
        Task.Run(async () => { await server.StartAsync(); });

        do
        {
            await Task.Delay(25);
        } while (server.State != Server.ServerState.started);

        var protocolClient = new Protocol("localhost", 10011, 3, new TimeSpan(0, 0, 2),false, "abc123abc");

        var message = "Der var engang i lille gris";
        
        var response = await protocolClient.SendSystemMessage(message);

        response.Should().Be(Bejo.DistributedCollections.Helpers.Helper.Hash(Encoding.UTF8.GetBytes(message)).ToString());
    }
    [Fact]
    public async Task EncryptionTest_WrongPAssword()
    {
        var protocolServer = new Protocol();
        var server = new Server(10020, false, new List<string>{"abc123abc"}, protocolServer.MessageHandler);
        Task.Run(async () => { await server.StartAsync(); });

        do
        {
            await Task.Delay(25);
        } while (server.State != Server.ServerState.started);

        var protocolClient = new Protocol("localhost", 10020, 3, new TimeSpan(0, 0, 2),false, "123aba321");

        var message = "Der var engang i lille gris";
        
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () => await protocolClient.SendSystemMessage(message));
        exception.Message.Should().StartWith("Error from server:");
        exception.Message.Should().Contain("Failed to decrypt message.");
    }
}