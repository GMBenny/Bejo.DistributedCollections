using FluentAssertions;
using Xunit.Abstractions;

namespace Bejo.DistributedCollections_Tests;

public class DistributedDictionaryTest
{
    private readonly ITestOutputHelper _output;
    public DistributedDictionaryTest(ITestOutputHelper output)
    {
        _output = output;
    }

    #region CREATE - Port: 1200*
    [Fact]
    public async Task Add_NewData_DataReadable()
    {
        var (dict1, dict2) = await CreateDistributedDictionary(12000, 12001);

        dict1.Add(1, "one");

        dict1[1].Should().Be("one");

        await IDictionaryHelper.AwaitValue(dict2, 1);

        dict2[1].Should().Be("one");
    }

    [Fact]
    public async Task Add_ExistingData_DataReadable()
    {
        var (dict1, dict2) = await CreateDistributedDictionary(12002, 12003);

        // Add data
        dict1.Add(1, "one");

        // Ensure data is local
        dict1[1].Should().Be("one");

        // Ensure data is replicated
        await IDictionaryHelper.AwaitValue(dict2, 1);
        dict2[1].Should().Be("one");

        // Try to add the same key again
        var exception1 = await Assert.ThrowsAsync<ArgumentException>(async () => { dict1.Add(1, "ONE"); });
        exception1.Message.Should().Be("An item with the same key has already been added.");

        var exception2 = await Assert.ThrowsAsync<ArgumentException>(async () => { dict2.Add(1, "ONE"); });
        exception2.Message.Should().Be("An item with the same key has already been added.");
    }

    [Fact]
    public async Task TryAdd_NewData_DataReadable()
    {
        var (dict1, dict2) = await CreateDistributedDictionary(12004, 12005);

        var b1 = dict1.TryAdd(1, "one");

        b1.Should().BeTrue();
        dict1[1].Should().Be("one");

        await IDictionaryHelper.AwaitValue(dict2, 1);
        dict2[1].Should().Be("one");
    }

    [Fact]
    public async Task TryAdd_ExistingData_ReturnsFalse()
    {
        var (dict1, dict2) = await CreateDistributedDictionary(12006, 12007);

        dict1.Add(1, "one");
        var b1 = dict1.TryAdd(1, "ONE");

        b1.Should().BeFalse();
        
        dict1[1].Should().Be("one");

        await IDictionaryHelper.AwaitValue(dict2, 1);
        dict2[1].Should().Be("one");
    }

    [Fact]
    public async Task AddKeyValuePair_NewData_DataReadable()
    {
        var (dict1, dict2) = await CreateDistributedDictionary(12008, 12009);

        dict1.Add(new KeyValuePair<int, string>(1, "one"));

        dict1[1].Should().Be("one");

        await IDictionaryHelper.AwaitValue(dict2, 1);

        dict2[1].Should().Be("one");
    }
    #endregion

    #region READ - Port: 1203*
    [Fact]
    public async Task Read_ExistingData_DataReadable()
    {
        var (dict1, dict2) = await CreateDistributedDictionary(12031, 12032);

        dict1.Add(1, "one");
        dict1.Add(2, "two");
        dict1.Add(3, "three");

        // Indexer
        dict1[1].Should().Be("one");
        dict1[2].Should().Be("two");
        dict1[3].Should().Be("three");

        // TryGetValue
        var bl1 = dict1.TryGetValue(1, out var lvalue1);
        bl1.Should().BeTrue();
        lvalue1.Should().Be("one");

        var bl2 = dict1.TryGetValue(2, out var lvalue2);
        bl2.Should().BeTrue();
        lvalue2.Should().Be("two");

        var bl3 = dict1.TryGetValue(3, out var lvalue3);
        bl3.Should().BeTrue();
        lvalue3.Should().Be("three");

        var bl4 = dict1.TryGetValue(4, out var lvalue4);
        bl4.Should().BeFalse();
        lvalue4.Should().BeNull();

        // ContainsKey 
        dict1.ContainsKey(1).Should().BeTrue();
        dict1.ContainsKey(2).Should().BeTrue();
        dict1.ContainsKey(3).Should().BeTrue();
        dict1.ContainsKey(4).Should().BeFalse();

        // Contains
        dict1.Contains(new KeyValuePair<int, string>(1, "one")).Should().BeTrue();
        dict1.Contains(new KeyValuePair<int, string>(2, "two")).Should().BeTrue();
        dict1.Contains(new KeyValuePair<int, string>(3, "three")).Should().BeTrue();

        dict1.Contains(new KeyValuePair<int, string>(2, "three")).Should().BeFalse();
        dict1.Contains(new KeyValuePair<int, string>(3, "two")).Should().BeFalse();
        dict1.Contains(new KeyValuePair<int, string>(4, "four")).Should().BeFalse();

        // Keys
        dict1.Keys.Should().BeEquivalentTo(new List<int> { 1, 2, 3 });

        // Values
        dict1.Values.Should().BeEquivalentTo(new List<string> { "one", "two", "three" });

        // Count
        dict1.Count.Should().Be(3);

        // Enumeration
        int li = 0;
        foreach (var item in dict1)
        {
            li++;
            if(li == 1)
            {
                item.Key.Should().Be(1);
                item.Value.Should().Be("one");
            }
            else if(li == 2)
            {
                item.Key.Should().Be(2);
                item.Value.Should().Be("two");
            }
            else if(li == 3)
            {
                item.Key.Should().Be(3);
                item.Value.Should().Be("three");
            }
        }

        // *** Ensure replication ***
        await Task.Delay(100);

        // Indexer
        dict2[1].Should().Be("one");
        dict2[2].Should().Be("two");
        dict2[3].Should().Be("three");

        // TryGetValue
        var br1 = dict1.TryGetValue(1, out var rvalue1);
        br1.Should().BeTrue();
        rvalue1.Should().Be("one");

        var br2 = dict1.TryGetValue(2, out var rvalue2);
        br2.Should().BeTrue();
        rvalue2.Should().Be("two");

        var br3 = dict1.TryGetValue(3, out var rvalue3);
        br3.Should().BeTrue();
        rvalue3.Should().Be("three");

        var br4 = dict1.TryGetValue(4, out var rvalue4);
        br4.Should().BeFalse();
        rvalue4.Should().BeNull();

        // ContainsKey 
        dict2.ContainsKey(1).Should().BeTrue();
        dict2.ContainsKey(2).Should().BeTrue();
        dict2.ContainsKey(3).Should().BeTrue();
        dict2.ContainsKey(4).Should().BeFalse();

        // Contains
        dict2.Contains(new KeyValuePair<int, string>(1, "one")).Should().BeTrue();
        dict2.Contains(new KeyValuePair<int, string>(2, "two")).Should().BeTrue();
        dict2.Contains(new KeyValuePair<int, string>(3, "three")).Should().BeTrue();

        dict2.Contains(new KeyValuePair<int, string>(2, "three")).Should().BeFalse();
        dict2.Contains(new KeyValuePair<int, string>(3, "two")).Should().BeFalse();
        dict2.Contains(new KeyValuePair<int, string>(4, "four")).Should().BeFalse();

        // Keys
        dict2.Keys.Should().BeEquivalentTo(new List<int> { 1, 2, 3 });

        // Values
        dict2.Values.Should().BeEquivalentTo(new List<string> { "one", "two", "three" });

        // Count
        dict2.Count.Should().Be(3);

        // Enumeration
        int ri = 0;
        foreach (var item in dict1)
        {
            ri++;
            if (ri == 1)
            {
                item.Key.Should().Be(1);
                item.Value.Should().Be("one");
            }
            else if (ri == 2)
            {
                item.Key.Should().Be(2);
                item.Value.Should().Be("two");
            }
            else if (ri == 3)
            {
                item.Key.Should().Be(3);
                item.Value.Should().Be("three");
            }
        }
    }

    [Fact]
    public async Task Read_MissingData_ThrowsException()
    {
        var (dict1, dict2) = await CreateDistributedDictionary(12033, 12034);

        await Task.Delay(100);

        // Indexer
        var exception1 = await Assert.ThrowsAsync<KeyNotFoundException>(async () => { dict1[1].Should().Be("one"); });
        exception1.Message.Should().Be("The given key was not present in the dictionary.");

        var exception2 = await Assert.ThrowsAsync<KeyNotFoundException>(async () => { dict2[1].Should().Be("one"); });
        exception2.Message.Should().Be("The given key was not present in the dictionary.");

        // TryGetValue
        var br1 = dict1.TryGetValue(1, out var rvalue1);
        br1.Should().BeFalse();
        rvalue1.Should().BeNull();

        var br2 = dict2.TryGetValue(1, out var rvalue2);
        br2.Should().BeFalse();
        rvalue2.Should().BeNull();

        // ContainsKey 
        dict1.ContainsKey(1).Should().BeFalse();
        dict2.ContainsKey(1).Should().BeFalse();

        // Contains
        dict1.Contains(new KeyValuePair<int, string>(1, "one")).Should().BeFalse();
        dict2.Contains(new KeyValuePair<int, string>(1, "one")).Should().BeFalse();

        // Keys
        dict1.Keys.Should().BeEquivalentTo(new List<int> { });
        dict2.Keys.Should().BeEquivalentTo(new List<int> { });

        // Values
        dict1.Values.Should().BeEquivalentTo(new List<string> { });
        dict2.Values.Should().BeEquivalentTo(new List<string> { });

        // Count
        dict1.Count.Should().Be(0);
        dict2.Count.Should().Be(0);

        // Enumeration
        int r1 = 0;
        foreach (var item in dict1)
        {
            r1++;
        }
        r1.Should().Be(0);

        int r2 = 0;
        foreach (var item in dict2)
        {
            r2++;
        }
        r2.Should().Be(0);
    }
    #endregion

    #region UPDATE - Port: 1202*
    [Fact]
    public async Task Update_ExistingKey_DataReadable()
    {
        var (dict1, dict2) = await CreateDistributedDictionary(12021, 12022);

        dict1.TryAdd(1, "one");

        await Task.Delay(100);

        dict2[1] = "ONE";

        await Task.Delay(100);

        dict1[1].Should().Be("ONE");
    }

    [Fact]
    public async Task Update_MissingKey_ReturnsError()
    {
        var (dict1, dict2) = await CreateDistributedDictionary(12023, 12024);

        await Task.Delay(100);

        var exception = await Assert.ThrowsAsync<KeyNotFoundException>(async () => { dict1[1].Should().Be("ONE"); });
        exception.Message.Should().Be("The given key was not present in the dictionary.");
    }
    #endregion

    #region DELETE - Port: 1201*
    [Fact]
    public async Task Remove_ExistingKey_DataRemoved()
    {
        var (dict1, dict2) = await CreateDistributedDictionary(12010, 12011);

        // Insert data
        dict1.TryAdd(1, "one");

        // Ensure data is local
        dict1[1].Should().Be("one");

        // Ensure data is replicated
        await IDictionaryHelper.AwaitValue(dict2, 1);
        dict2[1].Should().Be("one");

        // Remove data
        var b = dict2.Remove(1);

        b.Should().BeTrue();

        // Ensure data is removed locally
        dict2.ContainsKey(1).Should().BeFalse();

        // Ensure data is removed remotely
        await Task.Delay(100);
        dict1.ContainsKey(1).Should().BeFalse();
    }

    [Fact]
    public async Task Remove_NonexistingKey_DataUnchanged()
    {
        var (dict1, dict2) = await CreateDistributedDictionary(12012, 12013);

        // Insert data
        dict1.TryAdd(1, "one");

        // Ensure data is local
        dict1[1].Should().Be("one");

        // Ensure data is replicated
        await Task.Delay(100);
        dict2[1].Should().Be("one");

        // Remove data
        var b = dict2.Remove(2);

        b.Should().BeFalse();

        // Ensure data is removed locally
        dict2.ContainsKey(1).Should().BeTrue();

        // Ensure data is removed remotely
        await Task.Delay(100);
        dict1.ContainsKey(1).Should().BeTrue();
    }

    [Fact]
    public async Task RemoveKeyValuePair_ExistingKey_DataRemoved()
    {
        var (dict1, dict2) = await CreateDistributedDictionary(12014, 12015);

        // Insert data
        dict1.TryAdd(1, "one");

        // Ensure data is local
        dict1[1].Should().Be("one");

        // Ensure data is replicated
        await Task.Delay(100);
        dict2[1].Should().Be("one");

        // Remove data
        var b = dict2.Remove(new KeyValuePair<int, string>(1, "one"));

        b.Should().BeTrue();

        // Ensure data is removed locally
        dict2.ContainsKey(1).Should().BeFalse();

        // Ensure data is removed remotely
        await Task.Delay(100);
        dict1.ContainsKey(1).Should().BeFalse();
    }

    [Fact]
    public async Task RemoveKeyValuePair_WrongKey_DataUnchanged()
    {
        var (dict1, dict2) = await CreateDistributedDictionary(12016, 12017);

        // Insert data
        dict1.TryAdd(1, "one");

        // Ensure data is local
        dict1[1].Should().Be("one");

        // Ensure data is replicated
        await IDictionaryHelper.AwaitValue(dict2, 1);
        dict2[1].Should().Be("one");

        // Remove data
        var b = dict2.Remove(new KeyValuePair<int, string>(2, "one"));  // Wrong key

        b.Should().BeFalse();

        // Ensure data is removed locally
        dict2.ContainsKey(1).Should().BeTrue();

        // Ensure data is removed remotely
        await Task.Delay(100);
        dict1.ContainsKey(1).Should().BeTrue();
    }

    [Fact]
    public async Task RemoveKeyValuePair_WrongValue_DataUnchanged()
    {
        var (dict1, dict2) = await CreateDistributedDictionary(12018, 12019);

        // Insert data
        dict1.TryAdd(1, "one");

        // Ensure data is local
        dict1[1].Should().Be("one");

        // Ensure data is replicated
        await IDictionaryHelper.AwaitValue(dict2, 1);
        dict2[1].Should().Be("one");

        // Remove data
        var b = dict2.Remove(new KeyValuePair<int, string>(1, "ONE"));  // Wrong key

        b.Should().BeFalse();

        // Ensure data is removed locally
        dict2.ContainsKey(1).Should().BeTrue();

        // Ensure data is removed remotely
        await Task.Delay(100);
        dict1.ContainsKey(1).Should().BeTrue();
    }
    #endregion

    #region CONFLICT - Port: 121**
    [Fact]
    public async Task HandleConflict_ConflictingData_ResolvedData()
    {
        var (dict1, dict2) = await CreateDistributedDictionary(12191, 12192);

        dict1.TryAdd(1, "one");
        dict2.TryAdd(1, "ONE");

        await Task.Delay(100);

        dict1[1].Should().Be("ONE");
        dict2[1].Should().Be("ONE");
    }
    #endregion

    #region Replication speed - Port: 122**
    [Fact]
    public async Task ReplicationSpeed_ReplicatedData_DataReadable()
    {
        var (dict1, dict2) = await CreateDistributedDictionary(12200, 12201);

        dict1.Add(0, "init");

        await Task.Delay(100);

        dict1.Add(1, "one");
        dict1[1].Should().Be("one");

        var sw = new System.Diagnostics.Stopwatch();
        sw.Start();
        do
        {
            await Task.Delay(1);
        } while (!dict2.TryGetValue(1, out var value));

        sw.Stop();

        _output.WriteLine($"Replication time: {sw.Elapsed.TotalMilliseconds:F4} ms");
        sw.ElapsedMilliseconds.Should().BeLessThan(100);
    }
    #endregion

    #region Events - Port: 1221*
    [Fact]
    public async Task ChangeData_ChangedValue_EventRaised()
    {
        int? testKey = null;

        var rs1 = new DistributedCollections.ReplicationSet(
            new DistributedCollections.ReplicationSetSettings("rs1", 12210, false, null, [new("localhost", 12211, TimeSpan.FromSeconds(2))], 3, TimeSpan.FromSeconds(2)));
        var rs2 = new DistributedCollections.ReplicationSet(
            new DistributedCollections.ReplicationSetSettings("rs2", 12211, false, null, [new("localhost", 12210, TimeSpan.FromSeconds(2))], 3, TimeSpan.FromSeconds(2)));

        await WaitForReplicationSetToStart([rs1, rs2]);

        var dict1 = rs1.GetOrCreateDistributedDictionary<int, string>("d1", HandleConflict);
        var dict2 = rs2.GetOrCreateDistributedDictionary<int, string>("d1", HandleConflict);

        // Setup event handler for dict2
        dict2.Changed += (sender, args) =>
        {
            testKey = args.Key;
        };

        // Add data to dict1, which should trigger the event in dict2
        dict1.Add(12, "Twelve");

        // Wait for replication and event handling
        await IDictionaryHelper.AwaitValue(dict2, 12);

        // testKey should now be set to 12 
        testKey.Should().NotBeNull();
        testKey.Should().Be(12);
    }
    #endregion

    #region HELPER METHODS
    private string HandleConflict(int key, DistributedCollections.ConflictData<string> a, DistributedCollections.ConflictData<string> b)
    {
        return string.Compare(a.Value, b.Value, StringComparison.Ordinal) < 0 ? a.Value : b.Value;
    }

    private async Task WaitForReplicationSetToStart(DistributedCollections.ReplicationSet[] rs)
    {
        foreach (var r in rs)
        {
            while (!r.GetReplicationSetStatus().ServerStarted)
            {
                await Task.Delay(25);
            }
        }        
    }

    private async Task<(IDictionary<int, string> dict1, IDictionary<int, string> dict2)> CreateDistributedDictionary(int port1, int port2)
    {
        var rs1 = new DistributedCollections.ReplicationSet(
            new DistributedCollections.ReplicationSetSettings("rs1", port1, false, null, [new("localhost", port2, TimeSpan.FromSeconds(2))], 3, TimeSpan.FromSeconds(2)));
        var rs2 = new DistributedCollections.ReplicationSet(
            new DistributedCollections.ReplicationSetSettings("rs2", port2, false, null, [new("localhost", port1, TimeSpan.FromSeconds(2))], 3, TimeSpan.FromSeconds(2)));

        await WaitForReplicationSetToStart([rs1, rs2]);

        var dict1 = rs1.GetOrCreateDistributedDictionary<int, string>("d1", HandleConflict);
        var dict2 = rs2.GetOrCreateDistributedDictionary<int, string>("d1", HandleConflict);

        return (dict1, dict2);
    }
    #endregion
}