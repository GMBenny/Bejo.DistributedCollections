using Bejo.DistributedCollections;

// Lav cluster
var rs__1 = new ReplicationSet(new("rs__1", 11001, false, null,
    [new("localhost", 11002, TimeSpan.FromSeconds(2))],
    3, TimeSpan.FromSeconds(2)));

var rs__2 = new ReplicationSet(new("rs__2", 11002, false, null,
    [new("localhost", 11001, TimeSpan.FromSeconds(2))],
    3, TimeSpan.FromSeconds(2)));

// Add data til node 1
var d__1 = rs__1.GetOrCreateDistributedDictionary<string, string>("test", (k, v1, v2) => v1.Value);
var d__2 = rs__2.GetOrCreateDistributedDictionary<string, string>("test", (k, v1, v2) => v1.Value);

d__1.Add("key1", "value1");

await Task.Delay(500);


d__1.Add("key1", "vAlue1");
d__2.Add("key1", "Value1");

foreach (var item in d__2)
{
    Console.WriteLine($"På new RS2: {item.Key} - {item.Value}");
}

d__2.Remove("key1 *");

await Task.Delay(500);

// loop igennem værdierne - 1 kommer fra originalen og er gemt i fil, mens 2 kun kan komme fra sync
foreach (var item in d__1)
{
    Console.WriteLine($"På new RS1: {item.Key} - {item.Value}");
}




await Task.Delay(250000);






Console.WriteLine("Starting 2 replication sets...");

var rs1 = new ReplicationSet(new ("rs1", 10001,false, null,
    [new("localhost", 10002, TimeSpan.FromSeconds(2))],
    3,TimeSpan.FromSeconds(2), null, "aaa111"));

var rs2 = new ReplicationSet(new ("rs2", 10002, false, null,
    [new("localhost", 10001, TimeSpan.FromSeconds(2))], 
    3, TimeSpan.FromSeconds(2), null, "aaa111"));

do
{
    await Task.Delay(500);
} while (rs1.GetReplicationSetStatus().ThisStatus != ThisStatus.Online &&
         rs2.GetReplicationSetStatus().ThisStatus != ThisStatus.Online);

Console.WriteLine("Replication sets are online.");

var d1 = rs1.GetOrCreateDistributedDictionary<string, string>("test", (k, v1, v2) => v1.Value);

Console.WriteLine("Adding key1 to rs1/d1 with value 'Value 1 fra rs1/d1'");

d1.TryAdd("key1", "Value 1 fra rs1/d1");

Console.WriteLine("Starting 1 replication set...");

var rs3 = new ReplicationSet(new("rs3", 10003, false, null,
    [new("localhost", 10001, TimeSpan.FromSeconds(2))],
    3, TimeSpan.FromSeconds(2), null, "aaa111"));

do
{
    await Task.Delay(500);
} while (rs1.GetReplicationSetStatus().ThisStatus != ThisStatus.Online &&
         rs2.GetReplicationSetStatus().ThisStatus != ThisStatus.Online &&
         rs3.GetReplicationSetStatus().ThisStatus != ThisStatus.Online);

Console.WriteLine("Replication set rs3 is online.");

var d3 = rs3.GetOrCreateDistributedDictionary<string, string>("test", (k, v1, v2) => v1.Value);

Console.WriteLine("Trying to get key1 from new replication set");

if (d3.TryGetValue("key1", out var value))
{
    Console.WriteLine($"Value from rs3/d3: {value}");
}
else
{
    Console.WriteLine("Key not found in rs3/d3");
}

Console.WriteLine($"Status for rs1: {rs1.GetReplicationSetStatus().ThisStatus} | {string.Join(" - ", rs1.GetReplicationSetStatus().NetworkStats.Select(p=> p.NodeName + "," + p.LastStatus))}");
Console.WriteLine($"Status for rs2: {rs2.GetReplicationSetStatus().ThisStatus} | {string.Join(" - ", rs2.GetReplicationSetStatus().NetworkStats.Select(p=> p.NodeName + "," + p.LastStatus))}");
Console.WriteLine($"Status for rs3: {rs3.GetReplicationSetStatus().ThisStatus} | {string.Join(" - ", rs3.GetReplicationSetStatus().NetworkStats.Select(p=> p.NodeName + "," + p.LastStatus))}");

//Console.WriteLine("Stopping replication sets...");
//await rs2.LeaveAsync();


for (int i = 0; i < 1000; i++)
{
    Console.WriteLine($"Status for rs1: {rs1.GetReplicationSetStatus().ThisStatus} | {string.Join(" - ", rs1.GetReplicationSetStatus().NetworkStats.Select(p => p.NodeName + "," + p.LastStatus))}");
    Console.WriteLine($"Status for rs2: {rs2.GetReplicationSetStatus().ThisStatus} | {string.Join(" - ", rs2.GetReplicationSetStatus().NetworkStats.Select(p => p.NodeName + "," + p.LastStatus))}");
    Console.WriteLine($"Status for rs3: {rs3.GetReplicationSetStatus().ThisStatus} | {string.Join(" - ", rs3.GetReplicationSetStatus().NetworkStats.Select(p => p.NodeName + "," + p.LastStatus))}");

    await Task.Delay(2500);
}


await Task.Delay(250000);