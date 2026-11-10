using System.Buffers.Binary;

namespace Bejo.DistributedCollections.Network;

internal static class NetworkMessageFraming
{
    private const int LengthPrefixSize = sizeof(int);
    private const int MaxMessageSize = 16 * 1024 * 1024;

    internal static async Task WriteAsync(Stream stream, ReadOnlyMemory<byte> message, CancellationToken cancellationToken = default)
    {
        if (message.Length > MaxMessageSize)
        {
            throw new InvalidDataException($"Message size cannot exceed {MaxMessageSize} bytes.");
        }

        var lengthPrefix = new byte[LengthPrefixSize];
        BinaryPrimitives.WriteInt32BigEndian(lengthPrefix, message.Length);

        await stream.WriteAsync(lengthPrefix, cancellationToken);
        await stream.WriteAsync(message, cancellationToken);
        await stream.FlushAsync(cancellationToken);
    }

    internal static async Task<byte[]?> ReadAsync(Stream stream, CancellationToken cancellationToken = default)
    {
        var lengthPrefix = new byte[LengthPrefixSize];
        var prefixBytesRead = await stream.ReadAsync(lengthPrefix, cancellationToken);

        if (prefixBytesRead == 0)
        {
            return null;
        }

        await stream.ReadExactlyAsync(lengthPrefix.AsMemory(prefixBytesRead), cancellationToken);

        var messageLength = BinaryPrimitives.ReadInt32BigEndian(lengthPrefix);
        if (messageLength < 0 || messageLength > MaxMessageSize)
        {
            throw new InvalidDataException($"Received invalid message size: {messageLength} bytes.");
        }

        var message = new byte[messageLength];
        await stream.ReadExactlyAsync(message, cancellationToken);
        return message;
    }
}