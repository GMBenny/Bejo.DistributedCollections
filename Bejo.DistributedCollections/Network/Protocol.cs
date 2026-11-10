


using System.Diagnostics;
using System.Security.Cryptography;
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

    // Kaldes når der kommer noget ind på serveren
    internal byte[] MessageHandler(byte[] message)
    {
        var messageString = Encoding.UTF8.GetString(message.Take(40).ToArray()).ToUpper();

        // If this is a system message
        if(messageString.StartsWith("SYSTEM:"))
        {
            string type = new string(messageString.Skip("SYSTEM:".Length).ToArray());

            if(type.StartsWith("PING")) // A simple ping
            {
                var ticks = DateTime.UtcNow.Ticks;
                return Encoding.UTF8.GetBytes(ticks.ToString());
            }
            else if(type.StartsWith("MESSAGE:")) // A message containing a hash and the actual message content
            {
                try
                {
                    var content = VerifyAndExtractMessage("SYSTEM:MESSAGE:", message);

                    // Do stuff with content.Message

                    // Return this hash for verification on client side
                    return Encoding.UTF8.GetBytes(content.Hash.ToString());
                }
                catch (Exception ex)  // Flere ting kan have gået galt, men vi stoler ikke på beskeden
                {
                    return Encoding.UTF8.GetBytes($"Error: {ex.Message}");
                }
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
        // Ensure this instance is a client before sending a ping
        IsClientCheck();

        try
        {
            // Start measuring the round-trip time for the ping
            var stopWatch = new Stopwatch();

            var startTime = DateTime.UtcNow;
            stopWatch.Start();

            // Send the ping message to the server
            var response = await _client.SendAsync(Encoding.UTF8.GetBytes("SYSTEM:PING"));

            stopWatch.Stop();
            var endTime = DateTime.UtcNow;

            // Vi forventer response som serverens utc tid i ticks som en string
            if (long.TryParse(Encoding.UTF8.GetString(response), out long ticks))
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
    internal async Task<string> SendSystemMessage(string message)
    {
        // Ensure this instance is a client before sending a system message
        IsClientCheck();

        // Compute the hash of the message for integrity verification
        var messageHash = Helpers.Helper.Hash(Encoding.UTF8.GetBytes(message));

        // Prepare the message bytes to be sent, including the hash for verification
        var messageBytes = Encoding.UTF8.GetBytes("SYSTEM:MESSAGE:" + messageHash + ":" +  message);   

        // Send the message to the server and await the response
        var response = await _client.SendAsync(messageBytes);
        
        var responseString = Encoding.UTF8.GetString(response);
        ulong.TryParse(responseString, out ulong responseHash);

        if(responseHash != messageHash)
            throw new InvalidOperationException("Response hash does not match."); // TODO: Resend the message if the hash does not match
        else
            return responseString;
    }

    private void IsClientCheck()
    {
        if (_client == null)
        {
            throw new InvalidOperationException("This protocol instance is not a client.");
        }
    }
    private (byte[] Message, ulong Hash) VerifyAndExtractMessage(string type, byte[] message)
    {
        // Remove type from message
        var actualContent = message.Skip(type.Length).ToArray();
        
        string actualContent_debug = Encoding.UTF8.GetString(actualContent);

        // Find hash from client
        int colonIndex = Array.IndexOf(actualContent, (byte)':');
        if(colonIndex > 0)
        {
            if(!ulong.TryParse(Encoding.UTF8.GetString(actualContent, 0, colonIndex), out ulong messageHashValue))
                throw new InvalidOperationException("Invalid message hash.");
            
            byte[] actualMessageBytes = actualContent.Skip(Array.IndexOf(actualContent, (byte)':') + 1).ToArray();

            string actualMessage_debug = Encoding.UTF8.GetString(actualMessageBytes);

            var actualMessageHash = Helpers.Helper.Hash(actualMessageBytes);
            if(actualMessageHash == messageHashValue)
                return (actualMessageBytes, messageHashValue);
        }
        throw new InvalidOperationException("Message verification failed.");
    }
}