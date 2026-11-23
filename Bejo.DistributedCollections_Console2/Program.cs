using Bejo.DistributedCollections;

await Task.Delay(10000);

var rs1 = new ReplicationSet(new("rs-9", 10008, false, null,
    [new("localhost", 10001, TimeSpan.FromSeconds(2))],
    3, TimeSpan.FromSeconds(2), null, "aaa111"));


Console.WriteLine("Starting replication set rs1...");
await Task.Delay(30000);

Console.WriteLine($"Status for rs1: {rs1.GetReplicationSetStatus().ThisStatus} | {string.Join(", ", rs1.GetReplicationSetStatus().NetworkStats.Select(p => p.NodeName + "," + p.LastStatus))}");

Console.WriteLine("Stopping replication set rs1...");