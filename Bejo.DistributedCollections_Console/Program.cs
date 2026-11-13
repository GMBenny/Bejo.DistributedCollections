Console.WriteLine("Starting replication set...");

var rs1 = new Bejo.DistributedCollections.ReplicationSet(new Bejo.DistributedCollections.ReplicationSetSettings("rs1", 10001,true,["abc123abc"],true,[new("localhost", 10002, TimeSpan.FromSeconds(2), true)],3,TimeSpan.FromSeconds(2)));
var rs2 = new Bejo.DistributedCollections.ReplicationSet(new Bejo.DistributedCollections.ReplicationSetSettings("rs2", 10002, true, ["abc123abc"], true, [new("localhost", 10001, TimeSpan.FromSeconds(2), true)], 3, TimeSpan.FromSeconds(2)));

await Task.Delay(5000);

Console.WriteLine("All collections in rs1: " + string.Join(", ", rs1.GetAllCollections().Select(c => $"{c.Name} ({c.CollectionType})")));
Console.WriteLine("All collections in rs2: " + string.Join(", ", rs2.GetAllCollections().Select(c => $"{c.Name} ({c.CollectionType})")));

Console.WriteLine("Replication set started.");

var d1 = rs1.GetOrCreateDistributedDictionary<int, string>("myDictionary");

await Task.Delay(1000);

Console.WriteLine("All collections in rs1: " + string.Join(", ", rs1.GetAllCollections().Select(c => $"{c.Name} ({c.CollectionType})")));
Console.WriteLine("All collections in rs2: " + string.Join(", ", rs2.GetAllCollections().Select(c => $"{c.Name} ({c.CollectionType})")));

Console.WriteLine("Adding items to d1...");
d1.Add(1, "one");
d1.Add(2, "two");

foreach (var item in d1)
{
    Console.WriteLine($"D1: Key: {item.Key}, Value: {item.Value}");
}

await Task.Delay(5000);

Console.WriteLine("Create and read items from d2...");

var d2 = rs2.GetOrCreateDistributedDictionary<int, string>("myDictionary");

await Task.Delay(1000);

Console.WriteLine("All collections in rs1: " + string.Join(", ", rs1.GetAllCollections().Select(c => $"{c.Name} ({c.CollectionType})")));
Console.WriteLine("All collections in rs2: " + string.Join(", ", rs2.GetAllCollections().Select(c => $"{c.Name} ({c.CollectionType})")));

await Task.Delay(2000);

foreach (var item in d2) // <=========================== Denne gav 0 resultater - Den opretter en ny, men det skulle den ikke. Den skulle koble sig på den der er lavet allerede
{
    Console.WriteLine($"D2: Key: {item.Key}, Value: {item.Value}");
}

await Task.Delay(5000);

Console.WriteLine("Adding item to d1...");
d1.Add(3, "three");

await Task.Delay(5000);

Console.WriteLine("Read items from d2 after adding to d1...");
foreach (var item in d1) // <=============================== Her er tre resultater så noget virker <3
{
    Console.WriteLine($"D1: Key: {item.Key}, Value: {item.Value}");
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