using Bejo.DistributedCollections.Network;
using FluentAssertions;
using System.Text;

namespace Bejo.DistributedCollections_Tests;

public class ClientServerTest
{
    [Fact]
    public async Task SendData_NoServer_TimeoutException()
    {
        var c = new Client("localhost", 10001, 3, new TimeSpan(0, 0, 2), false, null);

        // Timeout exception as the connection cannot be established within the time, as there is no server running on the specified port.
        await Assert.ThrowsAsync<TimeoutException>(async () => await c.SendAsync(Encoding.UTF8.GetBytes("0:5")));
    }

    [Fact]
    public async Task SendData_NoEncryptionNoCompression_DataReversed()
    {
        var server = new Server(10002, false, null, message => message.Reverse().ToArray());

        await TestHelpers.WaitForServerToStartAsync(server);

        var c = new Client("localhost", 10002, 3, new TimeSpan(0, 0, 2), false, null);

        var r = await c.SendAsync(Encoding.UTF8.GetBytes($"0:5"));
        string response = Encoding.UTF8.GetString(r);

        response.Should().Be("5:0");
    }

    [Fact]
    public async Task SendData_NoEncryptionCompression_ErrorMessage()
    {
        var server = new Server(10003, true, null, message => message.Reverse().ToArray());

        await TestHelpers.WaitForServerToStartAsync(server);

        var c = new Client("localhost", 10003, 3, new TimeSpan(0, 0, 2), false, null);

        var r = await c.SendAsync(Encoding.UTF8.GetBytes($"0:5"));
        string response = Encoding.UTF8.GetString(r);

        response.Should().Be("Failed to decompress message.");
    }

    [Fact]
    public async Task SendData_SinglePasswordEncryptionNoCompression_DataReversed()
    {
        var server = new Server(10004, false, ["abc123abc"], message => message.Reverse().ToArray());

        await TestHelpers.WaitForServerToStartAsync(server);

        var c = new Client("localhost", 10004, 3, new TimeSpan(0, 0, 2), false, "abc123abc");

        var r = await c.SendAsync(Encoding.UTF8.GetBytes($"0:5"));
        string response = Encoding.UTF8.GetString(r);

        response.Should().Be("5:0");
    }

    [Fact]
    public async Task SendData_MultiplePasswordEncryptionNoCompression_DataReversed()
    {
        var server = new Server(10005, false, ["123454321", "abc123abc"], message => message.Reverse().ToArray());

        await TestHelpers.WaitForServerToStartAsync(server);

        var c = new Client("localhost", 10005, 3, new TimeSpan(0, 0, 2), false, "abc123abc");

        var r = await c.SendAsync(Encoding.UTF8.GetBytes($"0:5"));
        string response = Encoding.UTF8.GetString(r);

        response.Should().Be("5:0");
    }

    [Fact]
    public async Task SendData_CompressionMismatchA_ErrorMessage()
    {
        var server = new Server(10006, true, null, message => message.Reverse().ToArray());

        await TestHelpers.WaitForServerToStartAsync(server);

        var c = new Client("localhost", 10006, 3, new TimeSpan(0, 0, 2), false, null);

        var r = await c.SendAsync(Encoding.UTF8.GetBytes($"0:5"));
        string response = Encoding.UTF8.GetString(r);

        response.Should().Be("Failed to decompress message.");
    }
    [Fact]
    public async Task SendData_CompressionMismatchB_ReversedCompressedData()
    {
        var server = new Server(10010, false, null, message => message.Reverse().ToArray());

        await TestHelpers.WaitForServerToStartAsync(server);

        var c = new Client("localhost", 10010, 3, new TimeSpan(0, 0, 2), true, null);

        var r = await c.SendAsync(Encoding.UTF8.GetBytes($"0:5"));
        string response = Encoding.UTF8.GetString(r);

        response.Should().NotBe("5:0"); // The compressed data, are not decompressed, but reversed, so it should not be equal to "5:0"
    }
    [Fact]
    public async Task SendData_EncryptionMismatchA_ReversedEncryptedData()
    {
        var server = new Server(10007, false, null, message => message.Reverse().ToArray());

        await TestHelpers.WaitForServerToStartAsync(server);

        var c = new Client("localhost", 10007, 3, new TimeSpan(0, 0, 2), false, "abc123abc");

        var r = await c.SendAsync(Encoding.UTF8.GetBytes($"0:5"));
        string response = Encoding.UTF8.GetString(r);

        response.Should().NotBe("5:0"); // The server does not have the password, so it cannot decrypt the message, and will return the original message, which is not equal to "5:0"
    }

    [Fact]
    public async Task SendData_EncryptionMismatchB_ErrorMessage()
    {
        var server = new Server(10008, false, ["abc123abc"], message => message.Reverse().ToArray());

        await TestHelpers.WaitForServerToStartAsync(server);

        var c = new Client("localhost", 10008, 3, new TimeSpan(0, 0, 2), false, null);

        var r = await c.SendAsync(Encoding.UTF8.GetBytes($"0:5"));
        string response = Encoding.UTF8.GetString(r);

        response.Should().Be("Failed to decrypt message.");
    }

    [Fact]
    public async Task SendData_EncryptionMismatchC_ErrorMessage()
    {
        var server = new Server(10009, false, ["123abc123"], message => message.Reverse().ToArray());

        await TestHelpers.WaitForServerToStartAsync(server);

        var c = new Client("localhost", 10009, 3, new TimeSpan(0, 0, 2), false, null);

        var r = await c.SendAsync(Encoding.UTF8.GetBytes($"0:5"));
        string response = Encoding.UTF8.GetString(r);

        response.Should().Be("Failed to decrypt message.");
    }
}