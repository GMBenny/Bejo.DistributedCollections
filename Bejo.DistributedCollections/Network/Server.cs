using System.Net;
using System.Net.Sockets;

namespace Bejo.DistributedCollections.Network;

internal class Server
{
    internal enum ServerState { Starting, Started, Stopped, Faulted }

    private volatile ServerState _state = ServerState.Stopped;
    internal ServerState State => _state;

    private readonly int _port;
    private TcpListener? _listener;
    private readonly bool _compress;
    private readonly string[]? _masterKeys;

    private readonly Func<byte[], byte[]> _handleRequest;
    private readonly Func<byte[], bool, ConnectionHandlerResult>? _handleAuthenticatedRequest;
    private readonly Func<byte[], bool, string?, ConnectionHandlerResult>? _handleAuthenticatedRequestWithAddress;

    internal Server(int port, bool compress, string[]? masterKeys, Func<byte[], byte[]> handleRequest)
    {
        _port = port;
        _handleRequest = handleRequest;
        _compress = compress;
        _masterKeys = masterKeys;
    }

    internal Server(int port, bool compress, string[]? masterKeys, Func<byte[], bool, ConnectionHandlerResult> handleRequest)
    {
        _port = port;
        _handleRequest = static _ => throw new InvalidOperationException("The authenticated request handler must be used.");
        _handleAuthenticatedRequest = handleRequest;
        _compress = compress;
        _masterKeys = masterKeys;
    }

    internal Server(int port, bool compress, string[]? masterKeys, Func<byte[], bool, string?, ConnectionHandlerResult> handleRequest)
    {
        _port = port;
        _handleRequest = static _ => throw new InvalidOperationException("The authenticated request handler must be used.");
        _handleAuthenticatedRequestWithAddress = handleRequest;
        _compress = compress;
        _masterKeys = masterKeys;
    }

    // Start the server and listen for incoming connections
    internal async Task StartAsync(CancellationToken cancellationToken = default)
    {
        // Set the server state to Starting
        _state = ServerState.Starting;

        try
        {
            // Create a TCP listener to listen for incoming connections on the specified port
            _listener = new TcpListener(IPAddress.IPv6Any, _port);
            _listener.Server.DualMode = true;
            _listener.Start();

            // Set the server state to Started
            _state = ServerState.Started;

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    // Wait for an incoming client connection
                    var client = await _listener.AcceptTcpClientAsync(cancellationToken);
                    _ = HandleClientAsync(client, cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (SocketException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
            }
        }
        catch
        {
            // If an exception occurs during server startup, set the state to Faulted and rethrow the exception
            _state = ServerState.Faulted;
            throw;
        }
        finally
        {
            // Stop the listener and set the state to Stopped if it is not already Faulted
            _listener?.Stop();
            _listener = null;
            if (_state != ServerState.Faulted)
                _state = ServerState.Stopped;
        }
    }

    // Handle communication with a connected client
    private async Task HandleClientAsync(TcpClient client, CancellationToken cancellationToken)
    {
        var remoteEndPoint = client.Client.RemoteEndPoint;

        using (client)
        {
            try
            {
                await using var stream = client.GetStream();
                var authenticated = _handleAuthenticatedRequest == null;

                while (!cancellationToken.IsCancellationRequested)
                {
                    // Wait for and read a message from the client
                    var message = await NetworkMessageFraming.ReadAsync(stream, cancellationToken);

                    if (message == null)
                    {
                        break;  // Close the connection to the client by breaking the while loop
                    }
                    else
                    {
                        // If encryption is enabled, attempt to decrypt the message.
                        if(_masterKeys != null && _masterKeys.Length > 0)
                        {
                            byte[]? messageDecrypted = null;

                            int i =0;
                            do
                            {
                                messageDecrypted = Helpers.Helper.Decrypt(message, _masterKeys[i]);
                                if (messageDecrypted != null)
                                    message = messageDecrypted;
                                else
                                    i++;
                            } while (messageDecrypted == null && i < _masterKeys.Length);

                            // If decryption fails, send an error message to the client.
                            if (messageDecrypted == null)
                            {
                                message = null;
                                await NetworkMessageFraming.WriteAsync(stream, System.Text.Encoding.UTF8.GetBytes("Failed to decrypt message."), cancellationToken);
                            }
                        }

                        // If compression is enabled, decompress the message.
                        if(_compress && message != null)
                        {
                            message = Helpers.Helper.Decompress(message);

                            // If decompression fails, send an error message to the client.
                            if (message == null)
                                await NetworkMessageFraming.WriteAsync(stream, System.Text.Encoding.UTF8.GetBytes("Failed to decompress message."), cancellationToken);
                        }

                        if (message != null) {
                            byte[] response;
                            var closeConnection = false;


                            if (_handleAuthenticatedRequestWithAddress != null)
                            {
                                var result = _handleAuthenticatedRequestWithAddress(message, authenticated, remoteEndPoint?.ToString());
                                response = result.Response;
                                authenticated = result.Authenticated;
                                closeConnection = result.CloseConnection;
                            }
                            else if (_handleAuthenticatedRequest != null)
                            {
                                var result = _handleAuthenticatedRequest(message, authenticated);
                                response = result.Response;
                                authenticated = result.Authenticated;
                                closeConnection = result.CloseConnection;
                            }
                            else
                            {
                                response = _handleRequest(message);
                            }

                            await NetworkMessageFraming.WriteAsync(stream, response, cancellationToken);

                            if (closeConnection)
                                break;
                        }
                    }
                }
            }

            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // The server is shutting down.
            }
            catch (IOException ex)
            {
                // The client has, for example, disconnected.
                Console.WriteLine($"Client communication error: {ex.Message}");
            }
            catch (Exception ex)
            {
                // An unexpected error occurred while handling this client.
                Console.WriteLine($"Client handler error: {ex}");
            }
        }

        Console.WriteLine($"Client disconnected: {remoteEndPoint}");
    }
}

internal readonly record struct ConnectionHandlerResult(
    byte[] Response,
    bool Authenticated,
    bool CloseConnection);