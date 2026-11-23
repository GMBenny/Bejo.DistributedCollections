namespace Bejo.DistributedCollections_Tests;

internal static class IDictionaryHelper
{
    internal static async Task AwaitValue(IDictionary<int, string> dict2, int key)
    {
        for (int i = 0; i < 30; i++)
        {
            await Task.Delay(33);
            if (dict2.TryGetValue(key, out var value))
            {
                return;
            }
        }
    }

}
