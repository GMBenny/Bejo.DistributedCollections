using System.Net;
using System.Net.Sockets;

namespace Bejo.DistributedCollections.Network;

internal class Server
{
    internal enum ServerState { Starting, started, Stopped }

    internal ServerState State { get; private set; } = ServerState.Stopped;

    private readonly int _port;
    private TcpListener? _listener;
   
    private readonly Func<byte[], byte[]> _handleRequest;

    internal Server(int port, Func<byte[], byte[]> handleRequest)
    {
        _port = port;
        _handleRequest = handleRequest;
    }

    internal async Task StartAsync(CancellationToken cancellationToken = default)
    {
        State = ServerState.Starting;

        _listener = new TcpListener(IPAddress.Any, _port);
        _listener.Start();

        State = ServerState.started;

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
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
        finally
        {
            _listener.Stop();
            _listener = null;
        }
    }

    private async Task HandleClientAsync(TcpClient client, CancellationToken cancellationToken)
    {
        var remoteEndPoint = client.Client.RemoteEndPoint;

        Console.WriteLine($"Client connected: {remoteEndPoint}");

        using (client)
        {
            try
            {
                await using var stream = client.GetStream();

                while (!cancellationToken.IsCancellationRequested)
                {
                    var message = await NetworkMessageFraming.ReadAsync(stream, cancellationToken);

                    if (message == null)
                        break;

                    var response = _handleRequest(message);

                    await NetworkMessageFraming.WriteAsync(stream, response, cancellationToken);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Serveren lukker ned.
            }
            catch (IOException ex)
            {
                // Klienten har eksempelvis afbrudt forbindelsen.
                Console.WriteLine($"Client communication error: {ex.Message}");
            }
            catch (Exception ex)
            {
                // En uventet fejl i håndteringen af denne klient.
                Console.WriteLine($"Client handler error: {ex}");
            }
        }

        Console.WriteLine($"Client disconnected: {remoteEndPoint}");
    }
}