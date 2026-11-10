using Bejo.DistributedCollections.Network;
using FluentAssertions;
using System.Net.Sockets;
using System.Text;

namespace Bejo.DistributedCollections_Tests;

public class ProtocolTest
{
    [Fact]
    public async Task Sunshine()
    {
        var protocolServer = new Protocol();

        var server = new Server(10002, protocolServer.MessageHandler);
        Task.Run(async () => { await server.StartAsync(); });

        do
        {
            await Task.Delay(25);
        } while (server.State != Server.ServerState.started);

        var protocolClient = new Protocol("localhost", 10002, 3, new TimeSpan(0, 0, 2));
    }

    [Fact]
    public async Task SendPing()
    {
        var protocolServer = new Protocol();
        var server = new Server(10003, protocolServer.MessageHandler);
        Task.Run(async () => { await server.StartAsync(); });

        do
        {
            await Task.Delay(25);
        } while (server.State != Server.ServerState.started);

        var protocolClient = new Protocol("localhost", 10003, 3, new TimeSpan(0, 0, 2));
        var networkStat = await protocolClient.SendPing();
               
        networkStat.LastStatus.Should().Be(NetworkStatus.Online);
        networkStat.RoundtripTime.Should().BeLessThan(TimeSpan.FromSeconds(2));
    }

    [Fact]
    public async Task ReverseMessageWithHashCheck()
    {
        var protocolServer = new Protocol();
        var server = new Server(10004, protocolServer.MessageHandler);
        Task.Run(async () => { await server.StartAsync(); });

        do
        {
            await Task.Delay(25);
        } while (server.State != Server.ServerState.started);

        var protocolClient = new Protocol("localhost", 10004, 3, new TimeSpan(0, 0, 2));

        var message = "Der var engang i lille gris";
        
        var response = await protocolClient.SendSystemMessage(message);

        response.Should().Be(Bejo.DistributedCollections.Helpers.Helper.Hash(Encoding.UTF8.GetBytes(message)).ToString());
    }
}