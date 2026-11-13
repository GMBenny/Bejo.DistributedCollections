using System.Net;
using System.Net.Sockets;

namespace Bejo.DistributedCollections.Network;

internal class Server
{
    internal enum ServerState { Starting, started, Stopped }

    internal ServerState State { get; private set; } = ServerState.Stopped;

    private readonly int _port;
    private TcpListener? _listener;
    private readonly bool _compress;
    private readonly string[]? _passwords;

    private readonly Func<byte[], byte[]> _handleRequest;

    internal Server(int port, bool compress, string[]? passwords, Func<byte[], byte[]> handleRequest)
    {
        _port = port;
        _handleRequest = handleRequest;
        _compress = compress;
        _passwords = passwords;
    }

    internal async Task StartAsync(CancellationToken cancellationToken = default)
    {
        State = ServerState.Starting;

        _listener = new TcpListener(IPAddress.IPv6Any, _port);
        _listener.Server.DualMode = true;
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
                    // Afvent og læs beskred fra client
                    var message = await NetworkMessageFraming.ReadAsync(stream, cancellationToken);

                    if (message == null)
                    {
                        break;  // Luk forbindelsen til klienten ved at bryde while loopet
                    }
                    else
                    {
                        // Hvis der er valgt at kryptere, så forsøg at dekryptere beskeden.
                        if(_passwords != null && _passwords.Length > 0)
                        {
                            byte[]? messageDecrypted = null;

                            int i =0;
                            do
                            {
                                messageDecrypted = Helpers.Helper.Decrypt(message, _passwords[i]);
                                if (messageDecrypted != null)
                                    message = messageDecrypted;
                                else
                                    i++;
                            } while (messageDecrypted == null && i < _passwords.Length);

                            // Hvis dekrypteringen fejler, send en fejlbesked til klienten.
                            if (messageDecrypted == null)
                            {
                                message = null;
                                await NetworkMessageFraming.WriteAsync(stream, System.Text.Encoding.UTF8.GetBytes("Failed to decrypt message."), cancellationToken);
                            }
                        }

                        // Hvis der er valgt at komprimere, så dekomprimer beskeden.
                        if(_compress && message != null)
                        {
                            message = Helpers.Helper.Decompress(message);

                            // Hvis dekomprimeringen fejler, send en fejlbesked til klienten.
                            if (message == null)
                                await NetworkMessageFraming.WriteAsync(stream, System.Text.Encoding.UTF8.GetBytes("Failed to decompress message."), cancellationToken);
                        }

                        if (message != null) {
                            var response = _handleRequest(message);

                            await NetworkMessageFraming.WriteAsync(stream, response, cancellationToken);
                        }
                    }
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