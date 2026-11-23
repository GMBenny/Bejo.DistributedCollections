using Bejo.DistributedCollections.Contracts;
using System.Diagnostics;
using System.Text;
using System.Text.Json;

namespace Bejo.DistributedCollections.Network;

internal class Protocol
{
    // Either one is in use, depending on whether this instance is a client or a server
    private readonly Client? _client;
    private readonly ICommandReceiver? _receiver;
    private readonly string? _nodeName;
    private readonly string? _nodeKey;
    private readonly string? _acceptedNodeKey;
    private readonly Func<ThisStatus>? _statusProvider;
    private readonly Action<string, string, int, NetworkStatus>? _peerConnected;
    private readonly Action<string, NetworkStatus>? _peerStatusChanged;
    private readonly int _listenerPort;
    private string Address = "";

    // Constructor for Clients
    internal Protocol(string host, int port, int retryCount, TimeSpan timeout, bool compress, string? masterKey, string? nodeName = null, string? nodeKey = null, int listenerPort = 0)
    {
        _client = new Client(host, port, retryCount, timeout, compress, masterKey);
        Address = $"{host}:{port}";
        _nodeName = nodeName;
        _nodeKey = nodeKey;
        _listenerPort = listenerPort;
    }

    // Constructor for Servers
    internal Protocol(ICommandReceiver receiver, string? nodeName = null, string? nodeKey = null, Func<ThisStatus>? statusProvider = null, Action<string, string, int, NetworkStatus>? peerConnected = null, Action<string, NetworkStatus>? peerStatusChanged = null)
    {
        _receiver = receiver;
        _nodeName = nodeName;
        _acceptedNodeKey = nodeKey;
        _statusProvider = statusProvider;
        _peerConnected = peerConnected;
        _peerStatusChanged = peerStatusChanged;
    }

    #region Server-side methods for handling incoming messages
    internal byte[] MessageHandler(byte[] message)
    {
        return HandleMessage(message, authenticated: true).Response;
    }

    internal ConnectionHandlerResult AuthenticatedMessageHandler(byte[] message, bool authenticated)
    {
        return HandleMessage(message, authenticated);
    }

    internal ConnectionHandlerResult AuthenticatedMessageHandlerWithAddress(byte[] message, bool authenticated, string? remoteAddress)
    {
        return HandleMessage(message, authenticated, remoteAddress);
    }

    private ConnectionHandlerResult HandleMessage(byte[] message, bool authenticated, string? remoteAddress = null)
    {
        IsServerCheck();

        var messageString = Encoding.UTF8.GetString(message.Take(40).ToArray()).ToUpper();

        // If this is a system message
        if (messageString.StartsWith("SYSTEM:"))
        {
            string type = new string(messageString.Skip("SYSTEM:".Length).ToArray());

            if (type.StartsWith("HELLO:"))
            {
                try
                {
                    var content = VerifyAndExtractMessage("SYSTEM:HELLO:", message);
                    var hello = JsonSerializer.Deserialize<HelloMessage>(content.Message) ?? throw new InvalidOperationException("Invalid hello message.");

                    var accepted = string.Equals(_acceptedNodeKey, hello.NodeKey, StringComparison.Ordinal);

                    if (!accepted)
                        return Result("Error: Node key rejected.", authenticated: false, closeConnection: true);

                    var status = _statusProvider?.Invoke() ?? ThisStatus.Online;
                    _peerConnected?.Invoke(hello.NodeName, remoteAddress ?? hello.NodeName, hello.ListenerPort, ToNetworkStatus(status));
                    return Result($"OK:{_nodeName ?? string.Empty}:{_listenerPort}:{status}", authenticated: true, closeConnection: false);
                }
                catch (Exception ex)
                {
                    return Result($"Error: {ex.Message}", authenticated: false, closeConnection: true);
                }
            }

            // If not authenticated, reject all system and collection commands until HELLO is approved.
            if (!authenticated)
                return Result("Error: Authentication required.", authenticated: false, closeConnection: true);

            if (type.StartsWith("SYNC:"))
            {
                try
                {
                    // Initial sync uses a separate system protocol and not regular
                    // replication commands, so sync does not create replication loops.
                    if (_receiver is not ISyncReceiver syncReceiver)
                        throw new InvalidOperationException("Sync is not supported by this receiver.");

                    var content = VerifyAndExtractMessage("SYSTEM:SYNC:", message);
                    var request = JsonSerializer.Deserialize<SyncRequest>(content.Message) ?? throw new InvalidOperationException("Invalid sync request.");
                    var response = syncReceiver.HandleSyncRequest(request);
                    return Result(JsonSerializer.Serialize(response), authenticated, false);
                }
                catch (Exception ex)
                {
                    return Result($"Error: {ex.Message}", authenticated, false);
                }
            }
            else if (type.StartsWith("MEMBERSHIP:"))
            {
                try
                {
                    if (_receiver is not IMembershipReceiver membershipReceiver)
                        throw new InvalidOperationException("Membership is not supported by this receiver.");

                    var content = VerifyAndExtractMessage("SYSTEM:MEMBERSHIP:", message);
                    var update = JsonSerializer.Deserialize<MembershipUpdate>(content.Message)
                        ?? throw new InvalidOperationException("Invalid membership update.");
                    membershipReceiver.HandleMembership(update);
                    return Result("OK", authenticated, false);
                }
                catch (Exception ex)
                {
                    return Result($"Error: {ex.Message}", authenticated, false);
                }
            }
            else if (type.StartsWith("PING")) // A simple ping - returns utc-ticks
            {
                var status = _statusProvider?.Invoke().ToString() ?? ThisStatus.Online.ToString();
                if (remoteAddress != null && Enum.TryParse<ThisStatus>(status, out var remoteStatus))
                    _peerStatusChanged?.Invoke(remoteAddress, ToNetworkStatus(remoteStatus));
                return Result($"{DateTime.UtcNow.Ticks}:{status}", authenticated, false);
            }
            else if (type.StartsWith("STATUS"))
            {
                return Result(_statusProvider?.Invoke().ToString() ?? ThisStatus.Online.ToString(), authenticated, false);
            }
            else if (type.StartsWith("MESSAGE:")) // A message containing a hash and the actual message content
            {
                try
                {
                    var content = VerifyAndExtractMessage("SYSTEM:MESSAGE:", message);

                    var messageHash = Helpers.Helper.Hash(content.Message);

                    // Do stuff with content.Message
                    _receiver!.HandleCommand(new CommandDTO("", Encoding.UTF8.GetString(content.Message), ChangeType.Message, 0, 0));

                    // Return this hash for verification on client side
                    return Result(messageHash.ToString(), authenticated, false);
                }
                catch (Exception ex)  // Somethings went wrong, but we don't trust the message
                {
                    return Result($"Error: {ex.Message}", authenticated, false);
                }
            }
        }
        else if (messageString.StartsWith("COLLECTION:")) // If this is a collection command message
        {
            string type = new string(messageString.Skip("COLLECTION:".Length).ToArray());

            if (type.StartsWith("COMMAND:"))
            {
                try
                {
                    // Get content and verify hash
                    var content = VerifyAndExtractMessage("COLLECTION:COMMAND:", message);

                    // Get Command
                    var command = JsonSerializer.Deserialize<CommandDTO>(content.Message)
                        ?? throw new InvalidOperationException("Invalid command.");

                    // Handle command
                    _receiver!.HandleCommand(command);

                    // Return this hash for verification on client side
                    return Result(content.Hash.ToString(), authenticated, false);
                }
                catch (Exception ex)  // Somethings went wrong, but we don't trust the message
                {
                    return Result($"Error: {ex.Message}", authenticated, false);
                }
            }
        }

        // If we reach this point, the message was not recognized
        return Result("Error: Unrecognized message format.", authenticated, false);
    }

    private static ConnectionHandlerResult Result(string response, bool authenticated, bool closeConnection)
    {
        return new ConnectionHandlerResult(Encoding.UTF8.GetBytes(response), authenticated, closeConnection);
    }
    #endregion

    #region Client-side methods for sending messages to the server
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
            var response = await _client!.SendAsync(Encoding.UTF8.GetBytes("SYSTEM:PING"));

            stopWatch.Stop();
            var endTime = DateTime.UtcNow;

            // We expect the response to be the server's UTC time in ticks as a string
            var responseText = Encoding.UTF8.GetString(response);
            var responseParts = responseText.Split(':', 2);
            if (long.TryParse(responseParts[0], out long ticks))
            {
                if (startTime.Ticks < ticks && ticks < endTime.Ticks)
                {
                    var status = responseParts.Length == 2 && Enum.TryParse<ThisStatus>(responseParts[1], out var remoteStatus)
                        ? ToNetworkStatus(remoteStatus)
                        : NetworkStatus.Online;
                    return new NetworkStat(Address, endTime, status, stopWatch.Elapsed);
                }
                else
                    return new NetworkStat(Address, endTime, NetworkStatus.TimeProblem, stopWatch.Elapsed);
            }
            else
                return new NetworkStat(Address, DateTime.UtcNow, NetworkStatus.UnexpectedResult, stopWatch.Elapsed);
        }
        catch
        {
            return new NetworkStat(Address, DateTime.UtcNow, NetworkStatus.Offline, TimeSpan.Zero);
        }
    }

    internal async Task<HelloResult> SendHelloAsync()
    {
        IsClientCheck();

        if (_nodeName == null)
            throw new InvalidOperationException("A node name is required for a handshake.");

        // Prepare the hello message with the node name and optional node key
        var hello = new HelloMessage(_nodeName, _nodeKey, _listenerPort);
        
        // Serialize the hello message to JSON and compute its hash for integrity verification
        var message = JsonSerializer.SerializeToUtf8Bytes(hello);

        // Send the hello message to the server and await the response
        var hash = Helpers.Helper.Hash(message);

        // Prepare the message bytes to be sent, including the hash for verification
        var request = Encoding.UTF8.GetBytes($"SYSTEM:HELLO:{hash}:{Encoding.UTF8.GetString(message)}");

        // Send the message to the server and await the response
        var response = await _client!.SendAsync(request);

        // Convert the response bytes back to a string for processing
        var responseText = Encoding.UTF8.GetString(response);

        // Check if the response indicates an error
        if (!responseText.StartsWith("OK:", StringComparison.Ordinal))
            throw new InvalidOperationException(responseText);

        // Extract the status from the response and convert it to a NetworkStatus enum
        var responseParts = responseText.Split(':');
        if (responseParts.Length < 4 ||
            !int.TryParse(responseParts[2], out var listenerPort) ||
            !Enum.TryParse<ThisStatus>(responseParts[3], out var remoteStatus))
            throw new InvalidOperationException("Invalid handshake response.");

        // Return the corresponding NetworkStatus based on the remote status
        return new HelloResult(responseParts[1], listenerPort, ToNetworkStatus(remoteStatus));
    }

    internal async Task<SyncResponse> SendSyncRequestAsync(SyncRequest request)
    {
        IsClientCheck();

        var payload = JsonSerializer.SerializeToUtf8Bytes(request);
        var payloadHash = Helpers.Helper.Hash(payload);
        var message = Encoding.UTF8.GetBytes($"SYSTEM:SYNC:{payloadHash}:{Encoding.UTF8.GetString(payload)}");
        var response = await _client!.SendAsync(message);
        var responseText = Encoding.UTF8.GetString(response);

        if (responseText.StartsWith("Error:", StringComparison.Ordinal))
            throw new InvalidOperationException(responseText);

        return JsonSerializer.Deserialize<SyncResponse>(responseText) ?? throw new InvalidOperationException("Invalid sync response.");
    }

    internal async Task SendMembershipAsync(MembershipUpdate update)
    {
        IsClientCheck();

        var payload = JsonSerializer.SerializeToUtf8Bytes(update);
        var payloadHash = Helpers.Helper.Hash(payload);
        var message = Encoding.UTF8.GetBytes($"SYSTEM:MEMBERSHIP:{payloadHash}:{Encoding.UTF8.GetString(payload)}");
        var response = await _client.SendAsync(message);
        var responseText = Encoding.UTF8.GetString(response);

        if (responseText.StartsWith("Error:", StringComparison.Ordinal))
            throw new InvalidOperationException(responseText);
    }

    internal void Dispose()
    {
        _client?.Dispose();
    }

    private static NetworkStatus ToNetworkStatus(ThisStatus status)
    {
        return status switch
        {
            ThisStatus.LoadingData => NetworkStatus.LoadingData,
            ThisStatus.SyncingData => NetworkStatus.SyncingData,
            ThisStatus.Online => NetworkStatus.Online,
            ThisStatus.Faulted => NetworkStatus.UnexpectedResult,
            ThisStatus.Rejected => NetworkStatus.NoNodeKey,
            ThisStatus.Leaving => NetworkStatus.Offline,
            _ => NetworkStatus.Offline
        };
    }
    internal async Task<string> SendSystemMessage(string message)
    {
        // Ensure this instance is a client before sending a system message
        IsClientCheck();

        // Compute the hash of the message for integrity verification
        var messageHash = Helpers.Helper.Hash(Encoding.UTF8.GetBytes(message));

        // Prepare the message bytes to be sent, including the hash for verification
        var messageBytes = Encoding.UTF8.GetBytes("SYSTEM:MESSAGE:" + messageHash + ":" + message);

        // Send the message to the server and await the response
        var response = await _client!.SendAsync(messageBytes);

        var responseString = Encoding.UTF8.GetString(response);
        if (!ulong.TryParse(responseString, out ulong responseHash)) // If response is not a hash-code, then it is an error
            throw new InvalidOperationException($"Error from server: {responseString}");
        else if (responseHash != messageHash) // The hash on the transmitted message does not match the hash on the received message, so something went wrong
            throw new InvalidOperationException("Response hash does not match");
        else
            return responseString; // Return the hash as a string for verification
    }

    internal sealed record HelloMessage(string NodeName, string? NodeKey, int ListenerPort);
    internal sealed record HelloResult(string NodeName, int ListenerPort, NetworkStatus Status);
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
        var response = await _client!.SendAsync(messageBytes);

        var responseString = Encoding.UTF8.GetString(response);
        if (!ulong.TryParse(responseString, out ulong responseHash)) // If response is not a hash-code, then it is an error
            throw new InvalidOperationException($"Error from server: {responseString}");
        else if (responseHash != messageHash) // The hash on the transmitted message does not match the hash on the received message, so something went wrong
            throw new InvalidOperationException("Response hash does not match");
        else
            return responseString; // Return the hash as a string for verification
    }
    #endregion

    #region Local validation methods
    private void IsClientCheck()
    {
        if (_client == null)
        {
            throw new InvalidOperationException("This protocol instance is not a client.");
        }
    }
    private void IsServerCheck()
    {
        if (_receiver == null)
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
        if (colonIndex > 0)
        {
            if (!ulong.TryParse(Encoding.UTF8.GetString(actualContent, 0, colonIndex), out ulong messageHashValue))
                throw new InvalidOperationException("Invalid message hash.");

            byte[] actualMessageBytes = actualContent.Skip(Array.IndexOf(actualContent, (byte)':') + 1).ToArray();

            string actualMessage_debug = Encoding.UTF8.GetString(actualMessageBytes);

            var actualMessageHash = Helpers.Helper.Hash(actualMessageBytes);
            if (actualMessageHash == messageHashValue)
                return (actualMessageBytes, messageHashValue);
        }
        throw new InvalidOperationException("Message verification failed.");
    }
    #endregion
}