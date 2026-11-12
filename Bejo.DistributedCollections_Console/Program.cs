
using Bejo.DistributedCollections.Network;


var rs = new Bejo.DistributedCollections.ReplicationSet(
    new Bejo.DistributedCollections.ReplicationSetSettings
    (
        10001,
        true,
        ["abc123abc"],
        true,
        [
            new("localhost", 10002, TimeSpan.FromSeconds(2), true),
            new("localhost", 10003, TimeSpan.FromSeconds(2), true)
        ],
        3,
        TimeSpan.FromSeconds(2)
    ));

var myDictionary = rs.GetOrCreateDistributedDictionary<int, string>("myDictionary");


myDictionary.Add(1, "one");
myDictionary.Add(2, "two");

foreach (var item in myDictionary)
{
    Console.WriteLine($"Key: {item.Key}, Value: {item.Value}");
}




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