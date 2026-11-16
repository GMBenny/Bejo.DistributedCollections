using Bejo.DistributedCollections.Network;

namespace Bejo.DistributedCollections_Tests;

internal static class TestHelpers
{
    internal static async Task WaitForServerToStartAsync(Server s)
    {
        Task serverTask = s.StartAsync();

        do
        {
            await Task.Delay(100);
        } while (s.State != Server.ServerState.Started);
    }
}
