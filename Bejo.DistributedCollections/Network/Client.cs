using System.Net;
using System.Net.Sockets;

namespace Bejo.DistributedCollections.Network;

internal class Client : IDisposable
{
    // Lock
    private readonly SemaphoreSlim _sendLock = new(1, 1);

    // Server
    private readonly string _host;
    private readonly int _port;

    // Client
    private TcpClient? _client;
    private NetworkStream? _stream;
    private readonly int _retryCount;
    private readonly TimeSpan _timeout;

    // Compression and encryption settings
    private readonly bool _compress;
    private readonly string? _password;


    internal Client(string host, int port, int retryCount, TimeSpan timeout, bool compress, string? password)
    {
        _host = host;
        _port = port;
        _retryCount = retryCount;
        _timeout = timeout;
        _compress = compress;
        _password = password;
    }

    internal async Task<byte[]> SendAsync(byte[] message, CancellationToken cancellationToken = default)
    {
        if (_compress)
        {
            message = Helpers.Helper.Compress(message);
        }
        if (!string.IsNullOrWhiteSpace(_password))
        {
            message = Helpers.Helper.Encrypt(message, _password);
        }

        await _sendLock.WaitAsync(cancellationToken);

        try
        {
            for (int i = 0; i < _retryCount; i++)
            {
                try
                {
                    // Create a linked cancellation token with a 2-second timeout for the network operations
                    using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                    timeout.CancelAfter(_timeout);

                    if (_stream == null)
                    {
                        await ConnectAsync(timeout.Token);
                    }

                    await NetworkMessageFraming.WriteAsync(_stream!, message, timeout.Token);

                    var response = await NetworkMessageFraming.ReadAsync(_stream!, timeout.Token);

                    if (response == null)
                        throw new IOException("The remote node closed the connection without a response.");

                    return response;
                }
                catch (OperationCanceledException)
                {
                    Disconnect();

                    if (i == _retryCount - 1)
                        throw new TimeoutException("The remote node did not respond in time.");

                    await Task.Delay((i + 1) * 100, cancellationToken);
                }
                catch (Exception ex) when (ex is IOException || ex is SocketException)
                {
                    Disconnect();

                    if (i == _retryCount - 1)
                        throw;

                    await Task.Delay((i + 1) * 100, cancellationToken);
                }
            }

            throw new IOException("All retry attempts failed.");
        }
        finally
        {
            _sendLock.Release();
        }
    }

    private async Task ConnectAsync(CancellationToken cancellationToken)
    {
        _client = new TcpClient();
        await _client.ConnectAsync(_host, _port, cancellationToken);

        _stream = _client.GetStream();
    }

    private void Disconnect()
    {
        _stream?.Dispose();
        _stream = null;

        _client?.Dispose();
        _client = null;
    }

    public void Dispose()
    {
        _stream?.Dispose();
        _client?.Dispose();
        _sendLock.Dispose();
    }
}