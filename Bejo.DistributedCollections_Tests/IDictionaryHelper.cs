namespace Bejo.DistributedCollections_Tests;

internal static class IDictionaryHelper
{
    internal static async Task AwaitValue(IDictionary<int, string> dict2, int key)
    {
        for (int i = 0; i < 150; i++)
        {
            await Task.Delay(20);
            if (dict2.TryGetValue(key, out var value))
            {
                return;
            }
        }

        throw new TimeoutException($"The dictionary value for key '{key}' was not replicated in time.");
    }

    internal static async Task AwaitValue(IDictionary<int, string> dictionary, int key, string expectedValue)
    {
        for (int i = 0; i < 150; i++)
        {
            await Task.Delay(20);
            if (dictionary.TryGetValue(key, out var value) && value == expectedValue)
            {
                return;
            }
        }

        throw new TimeoutException($"The dictionary value for key '{key}' did not become '{expectedValue}' in time.");
    }

    internal static async Task AwaitMissing(IDictionary<int, string> dictionary, int key)
    {
        for (int i = 0; i < 150; i++)
        {
            await Task.Delay(20);
            if (!dictionary.ContainsKey(key))
            {
                return;
            }
        }

        throw new TimeoutException($"The dictionary value for key '{key}' was not removed in time.");
    }

}
