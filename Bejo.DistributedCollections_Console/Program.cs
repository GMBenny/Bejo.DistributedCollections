
using Bejo.DistributedCollections.Network;

Console.WriteLine("Starting server...");

var s = new Server(12345, message => message.Reverse().ToArray());

Task.Run(async () => await s.StartAsync());
//await s.StartAsync(); // Blocking kald

Console.WriteLine("Server starting...");

do
{
    Console.WriteLine($"Server state: {s.State}");
    await Task.Delay(1000);
} while (s.State != Server.ServerState.started);

Console.WriteLine("Server started.");

var c = new Client("localhost", 12345, 3, new TimeSpan(0, 0, 2));

for(int i = 0; i < 4; i++)
{
    var r = await c.SendAsync(System.Text.Encoding.UTF8.GetBytes($"{i}:{5-i}"));
    Console.WriteLine("Response: " + System.Text.Encoding.UTF8.GetString(r));
}




Console.WriteLine("End");
Console.ReadKey();