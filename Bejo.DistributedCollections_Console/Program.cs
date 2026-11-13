using Bejo.DistributedCollections;


void Print(ReplicationSetStatus replicationSetStatus)
{
    Console.WriteLine($"Replication set '{replicationSetStatus.NodeName}' status: {replicationSetStatus.ThisStatus}");
    foreach (var stat in replicationSetStatus.NetworkStats)
    {
        Console.WriteLine($"  Node: {stat.Address}, LastRoundtripUTC: {stat.LastRoundtripUTC}, LastStatus: {stat.LastStatus}, RoundtripTime: {stat.RoundtripTime}");
    }
}



Console.WriteLine("Starting 3 replication sets...");

var rs1 = new Bejo.DistributedCollections.ReplicationSet(new Bejo.DistributedCollections.ReplicationSetSettings("rs1", 10001,true,["abc123abc"],true,[new("localhost", 10002, TimeSpan.FromSeconds(2), true),new("localhost", 10003, TimeSpan.FromSeconds(2), true)],3,TimeSpan.FromSeconds(2)));
var rs2 = new Bejo.DistributedCollections.ReplicationSet(new Bejo.DistributedCollections.ReplicationSetSettings("rs2", 10002, true, ["abc123abc"], true, [new("localhost", 10001, TimeSpan.FromSeconds(2), true),new("localhost", 10003, TimeSpan.FromSeconds(2), true)], 3, TimeSpan.FromSeconds(2)));
var rs3 = new Bejo.DistributedCollections.ReplicationSet(new Bejo.DistributedCollections.ReplicationSetSettings("rs3", 10003, true, ["abc123abc"], true, [new("localhost", 10001, TimeSpan.FromSeconds(2), true),new("localhost", 10002, TimeSpan.FromSeconds(2), true)], 3, TimeSpan.FromSeconds(2)));

do
{
    Console.WriteLine($"Replication set status: rs1={rs1.GetReplicationSetStatus().ThisStatus}, rs2={rs2.GetReplicationSetStatus().ThisStatus}, rs3={rs3.GetReplicationSetStatus().ThisStatus}");

    await Task.Delay(25);
} while (rs1.GetReplicationSetStatus().ThisStatus != Bejo.DistributedCollections.ThisStatus.Started || 
         rs2.GetReplicationSetStatus().ThisStatus != Bejo.DistributedCollections.ThisStatus.Started || 
         rs3.GetReplicationSetStatus().ThisStatus  != Bejo.DistributedCollections.ThisStatus.Started);

string h1(int k, ConflictData<string> a, ConflictData<string> b)
{
    return b.Value;
}
string h2(int k, ConflictData<string> a, ConflictData<string> b)
{
    return b.Value;
}

var d1 = rs1.GetOrCreateDistributedDictionary<int, string>("myDictionary", h1);
var d2 = rs2.GetOrCreateDistributedDictionary<int, string>("myDictionary", h2);

d1.TryAdd(1, "one");
d2.TryAdd(1, "two");

Console.WriteLine($"D1: Key 1: {d1[1]} / {d2[1]}");

await Task.Delay(1000);

Console.WriteLine($"D1: Key 1: {d1[1]} / {d2[1]}");

await Task.Delay(1000);

Console.WriteLine($"D1: Key 1: {d1[1]} / {d2[1]}");

Console.WriteLine("All collections in each replication set:");

Console.WriteLine("All collections in rs1: " + string.Join(", ", rs1.GetAllCollections().Select(c => $"{c.Name} ({c.CollectionType})")));
Console.WriteLine("All collections in rs2: " + string.Join(", ", rs2.GetAllCollections().Select(c => $"{c.Name} ({c.CollectionType})")));
Console.WriteLine("All collections in rs3: " + string.Join(", ", rs3.GetAllCollections().Select(c => $"{c.Name} ({c.CollectionType})")));

Console.WriteLine("Adding items to d1...");
d1.Add(1, "one");
d1.Add(2, "two");

foreach (var item in d1)
{
    Console.WriteLine($"D1: Key: {item.Key}, Value: {item.Value}");
}
await Task.Delay(500);
foreach (var item in d2)
{
    Console.WriteLine($"D2: Key: {item.Key}, Value: {item.Value}");
}

await Task.Delay(1500);

Console.WriteLine("Create d3");

var d3 = rs3.GetOrCreateDistributedDictionary<int, string>("myDictionary", (key, localValue, distributedValue) => localValue.Value);
foreach (var item in d3)
{
    Console.WriteLine($"D3: Key: {item.Key}, Value: {item.Value}");
}


Print(rs1.GetReplicationSetStatus());
Print(rs2.GetReplicationSetStatus());
Print(rs3.GetReplicationSetStatus());


await Task.Delay(100000);

Console.WriteLine("Press any key to exit...");
Console.ReadKey();





/*
Console.WriteLine("Starting server...");

var s = new Server(12345, true, new List<string>{ "abc123abc", "123454321"}, message => message.Reverse().ToArray());

Task.Run(async () => await s.StartAsync());
//await s.StartAsync(); // Blocking kald

Console.WriteLine("Server starting...");

do
{
    Console.WriteLine($"Server state: {s.State}");
    await Task.Delay(1000);
} while (s.State != Server.ServerState.started);

Console.WriteLine("Server started.");

var c = new Client("localhost", 12345, 3, new TimeSpan(0, 0, 2), true, "123454321");

for(int i = 0; i < 4; i++)
{
    var r = await c.SendAsync(System.Text.Encoding.UTF8.GetBytes($"{i}:{5-i}"));
    Console.WriteLine("Response: " + System.Text.Encoding.UTF8.GetString(r));
}




Console.WriteLine("End");
Console.ReadKey();
*/