using Bejo.DistributedCollections.Network;

namespace Bejo.DistributedCollections_Tests;

public class ServerTest
{
    [Fact]
    public async Task ServerState_CancellationToken_ServerStateStopped()
    {
        var server = new Server(0, false, null, message => message);

        using var cancellation = new CancellationTokenSource();
        Assert.Equal(Server.ServerState.Stopped, server.State);

        var serverTask = server.StartAsync(cancellation.Token);
        Assert.Equal(Server.ServerState.Started, server.State);
        cancellation.Cancel();
        await serverTask.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(Server.ServerState.Stopped, server.State);
    }
}