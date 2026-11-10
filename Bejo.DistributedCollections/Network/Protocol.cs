


using System.Diagnostics;
using System.Text;

namespace Bejo.DistributedCollections.Network;

internal class Protocol
{
    private readonly Client? _client;

    // Constructor for Clients
    internal Protocol(string host, int port, int retryCount, TimeSpan timeout)
    {
        _client = new Client(host, port, retryCount, timeout);
    }

    // Constructor for Servers
    internal Protocol() //Func<byte[], byte[]> messageHandler)
    {
    }

    internal byte[] MessageHandler(byte[] message)
    {
        var messageString = Encoding.UTF8.GetString(message.Take(20).ToArray()).ToUpper();

        // If this is a system message
        if(messageString.StartsWith("SYSTEM:"))
        {
            if(messageString.StartsWith("SYSTEM:PING"))
            {
                var ticks = DateTime.UtcNow.Ticks;
                return Encoding.UTF8.GetBytes(ticks.ToString());
            }
        }
        else if(messageString.StartsWith("DICTIONARY:"))
        {
            // Handle other types of messages here
        }


        // If we reach this point, the message was not recognized
        return new byte[0];
    }

    internal async Task<NetworkStat> SendPing()
    {
        IsClientCheck();

        try
        {
            var stopWatch = new Stopwatch();

            var startTime = DateTime.UtcNow;

            stopWatch.Start();
            var response = await _client.SendAsync(Encoding.UTF8.GetBytes("SYSTEM:PING"));
            stopWatch.Stop();

            var endTime = DateTime.UtcNow;

            string responseStr = Encoding.UTF8.GetString(response);

            // Vi forventer response som serverens utc tid i ticks som en string
            if (long.TryParse(responseStr, out long ticks))
            {
                if(startTime.Ticks < ticks && ticks < endTime.Ticks)
                    return new NetworkStat(endTime, NetworkStatus.Online, stopWatch.Elapsed);
                else
                    return new NetworkStat(endTime, NetworkStatus.TimeProblem, stopWatch.Elapsed);
            }
            else
                return new NetworkStat(DateTime.UtcNow, NetworkStatus.UnexpectedResult, stopWatch.Elapsed);
        }
        catch
        {
            return new NetworkStat(DateTime.UtcNow, NetworkStatus.Offline, TimeSpan.Zero);
        }
    }


    private void IsClientCheck()
    {
        if (_client == null)
        {
            throw new InvalidOperationException("This protocol instance is not a client.");
        }
    }
}