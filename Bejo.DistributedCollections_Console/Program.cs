using Bejo.DistributedCollections;


Console.WriteLine("Starting 2 replication sets...");

var rs1 = new ReplicationSet(new ("rs1", 10001,true,["abc123abc"],
    [new("localhost", 10002, TimeSpan.FromSeconds(2))],
    3,TimeSpan.FromSeconds(2), "C:\\Benny\\PrivateProjekter\\Bejo.DistributedCollections\\Data\\RS1\\"));
var rs2 = new ReplicationSet(new ("rs2", 10002, true, ["abc123abc"], 
    [new("localhost", 10001, TimeSpan.FromSeconds(2))], 
    3, TimeSpan.FromSeconds(2), "C:\\Benny\\PrivateProjekter\\Bejo.DistributedCollections\\Data\\RS2\\"));

do
{
    await Task.Delay(25);
} while (new[] { rs1, rs2 }.Any(rs => !rs.GetReplicationSetStatus().ServerStarted));

var d1 = rs1.GetOrCreateDistributedDictionary<int, string>("myDictionary", (k, a, b) => a.CreatedTimestamp < b.CreatedTimestamp ? a.Value : b.Value);
var d2 = rs2.GetOrCreateDistributedDictionary<int, string>("myDictionary", (k, a, b) => a.CreatedTimestamp < b.CreatedTimestamp ? a.Value : b.Value);

await Task.Delay(1000);

foreach (var kvp in d1) 
{
    Console.WriteLine($"d1: Key: {kvp.Key}, Value: {kvp.Value}");
}
foreach (var kvp in d2)
{
    Console.WriteLine($"d2: Key: {kvp.Key}, Value: {kvp.Value}");
}

d1.Add(1, "one");
d2.Add(2, "TWO");
d1.Add(3, "three");
d2.Add(4, "FOUR");


await Task.Delay(1000);

Console.WriteLine("All collections in rs1: " + string.Join(", ", rs1.GetAllCollections().Select(c => $"{c.Name} ({c.CollectionType})")));
Console.WriteLine("All collections in rs2: " + string.Join(", ", rs2.GetAllCollections().Select(c => $"{c.Name} ({c.CollectionType})")));

foreach (var kvp in d1)
{
    Console.WriteLine($"d1: Key: {kvp.Key}, Value: {kvp.Value}");
}

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