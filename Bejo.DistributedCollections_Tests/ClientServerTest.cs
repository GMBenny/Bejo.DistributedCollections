using Bejo.DistributedCollections.Network;
using FluentAssertions;
using System.Net.Sockets;
using System.Text;

namespace Bejo.DistributedCollections_Tests;

public class ClientServerTest
{
    [Fact]
    public async Task Sunshine()
    {
        var s = new Server(10000, true, new List<string>{ "abc123abc", "123454321"}, message => message.Reverse().ToArray());

        Task.Run(async () => { 
            await s.StartAsync(); // Dette kald blokere tråden hvis det køres synkront
        });

        do
        {
            await Task.Delay(100);
        } while (s.State != Server.ServerState.started);

        var c = new Client("localhost", 10000, 3, new TimeSpan(0, 0, 2), true, "123454321");

        var r = await c.SendAsync(System.Text.Encoding.UTF8.GetBytes($"0:5"));
        string response = System.Text.Encoding.UTF8.GetString(r);

        response.Should().Be("5:0");
    }

    [Fact]
    public async Task Client_NoServer()
    {
        var c = new Client("localhost", 10001, 3, new TimeSpan(0, 0, 2), true, "123454321");

        await Assert.ThrowsAsync<SocketException>(async () => await c.SendAsync(Encoding.UTF8.GetBytes("0:5")));
    }
}
