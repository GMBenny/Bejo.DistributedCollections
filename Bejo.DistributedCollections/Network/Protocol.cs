using Bejo.DistributedCollections.Dictionary;
using System.Collections;
using System.Diagnostics;
using System.Reflection;
using System.Text;
using System.Text.Json;

namespace Bejo.DistributedCollections.Network;

internal class Protocol
{
    // Either one is in use, depending on whether this instance is a client or a server
    private readonly Client? _client;
    private readonly Dictionary<string, CollectionInfo>? _distributedCollections;
    private readonly ReplicationManager? _replicationManager;

    // Constructor for Clients
    internal Protocol(string host, int port, int retryCount, TimeSpan timeout, bool compress, string? password)
    {
        _client = new Client(host, port, retryCount, timeout, compress, password);
    }

    // Constructor for Servers
    internal Protocol(Dictionary<string, CollectionInfo> distributedCollections, ReplicationManager replicationManager)
    {
        _distributedCollections = distributedCollections;
        _replicationManager = replicationManager;
    }

    // Kaldes når der kommer noget ind på serveren
    internal byte[] MessageHandler(byte[] message)
    {
        IsServerCheck();

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
                    var messageHash = Helpers.Helper.Hash(content.Message);

                    // Return this hash for verification on client side

                    // TODO: Burde det ikke være messageHash og ikke content.Hash vi sender retur? Undersøg først om de er ens og om jeg laver hash af det rigtige data
                    return Encoding.UTF8.GetBytes(content.Hash.ToString());
                }
                catch (Exception ex)  // Flere ting kan have gået galt, men vi stoler ikke på beskeden
                {
                    return Encoding.UTF8.GetBytes($"Error: {ex.Message}");
                }
            }
        }
        else if(messageString.StartsWith("COLLECTION:"))
        {
            string type = new string(messageString.Skip("COLLECTION:".Length).ToArray());

            if (type.StartsWith("COMMAND:")) // A message containing a hash and the actual message content
            {
                try
                {
                    var content = VerifyAndExtractMessage("COLLECTION:COMMAND:", message);

                    var command = JsonSerializer.Deserialize<CommandDTO>(content.Message);

                    // Hvis command er opret en collection og den ikke allerede findes, så opret den ud fra medfølgende info
                    if (command.Action == ChangeType.CreateCollection && !_distributedCollections.TryGetValue(command.CollectionName, out var _))
                    {
                        var node = new DistributedCollectionNode<int, string>(command.CollectionName, _replicationManager);


                        var info = JsonSerializer.Deserialize<DictionaryTypeInfo>(command.JsonObject)!;

                        var keyType = Type.GetType(info.KeyType)!;
                        var valueType = Type.GetType(info.ValueType)!;

                        var dictionaryType = typeof(DistributedDictionary<,>).MakeGenericType(keyType, valueType);

                        //var dictionary = (IDistributedCollection)Activator.CreateInstance(dictionaryType, node);
                        var dictionary = (IDistributedCollection)Activator.CreateInstance(dictionaryType, BindingFlags.Instance | BindingFlags.NonPublic, binder: null, args: new object[] { node }, culture: null)!;

                        _distributedCollections.Add(command.CollectionName, new CollectionInfo(info.CollectionType, dictionary!, node));
                    }
                    // Og hvis collection allerede findes, så send command videre til den
                    else if (_distributedCollections.TryGetValue(command.CollectionName, out var collection))
                    {
                        // TODO: Er dette ok eller grimt? Bliver jeg for pragmatisk her?
                        collection.Node.CommandCallback(command);
                    }

                    // Return this hash for verification on client side
                    return Encoding.UTF8.GetBytes(content.Hash.ToString());
                }
                catch (Exception ex)  // Flere ting kan have gået galt, men vi stoler ikke på beskeden
                {
                    return Encoding.UTF8.GetBytes($"Error: {ex.Message}");
                }
            }
        }


        // If we reach this point, the message was not recognized
        return Encoding.UTF8.GetBytes($"Error: Unrecognized message format.");
    }

    internal async Task<NetworkStat> SendPingAsync()
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
        if(!ulong.TryParse(responseString, out ulong responseHash)) // If response is not a hash-code, then it is an error
            throw new InvalidOperationException($"Error from server: {responseString}");
        else if(responseHash != messageHash)
            throw new InvalidOperationException("Response hash does not match");
        else
            return responseString;
    }


    internal async Task<string> SendCommand(CommandDTO command)
    {
        // Ensure this instance is a client before sending a system message
        IsClientCheck();

        // Compute the hash of the message for integrity verification
        var messageText = JsonSerializer.Serialize(command);
        var message = Encoding.UTF8.GetBytes(messageText);
        var messageHash = Helpers.Helper.Hash(message);

        // Prepare the message bytes to be sent, including the hash for verification
        var messageBytes = Encoding.UTF8.GetBytes($"COLLECTION:COMMAND:{messageHash}:{messageText}");

        // Send the message to the server and await the response
        var response = await _client.SendAsync(messageBytes);

        var responseString = Encoding.UTF8.GetString(response);
        if (!ulong.TryParse(responseString, out ulong responseHash)) // If response is not a hash-code, then it is an error
            throw new InvalidOperationException($"Error from server: {responseString}");
        else if (responseHash != messageHash)
            throw new InvalidOperationException("Response hash does not match");
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
    private void IsServerCheck()
    {
        if (_distributedCollections == null || _replicationManager == null)
        {
            throw new InvalidOperationException("This protocol instance is not a server.");
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