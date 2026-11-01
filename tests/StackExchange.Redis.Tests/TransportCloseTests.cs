using System;
using System.Threading.Tasks;
using Xunit;

namespace StackExchange.Redis.Tests;

[RunPerProtocol]
public class TransportCloseTests(ITestOutputHelper output)
{
    [Fact]
    public async Task PeerCloseFailsOutstandingCommandsAndRaisesConnectionFailed()
    {
        using var server = new InProcessTestServer(output);
        var tunnel = new StreamTransportTunnel(server.Tunnel);
        var config = server.GetClientConfig(withPubSub: false, defaultOnly: true);
        config.Tunnel = tunnel;

        await using var conn = await ConnectionMultiplexer.ConnectAsync(config);
        var failed = new TaskCompletionSource<ConnectionFailedEventArgs>(TaskCreationOptions.RunContinuationsAsynchronously);
        conn.ConnectionFailed += (_, e) => failed.TrySetResult(e);

        var db = conn.GetDatabase();
        await db.PingAsync();

        // The next command is written to the transport but never reaches the server, so it is still
        // awaiting its reply when the server closes the connection.
        var transport = tunnel.Interactive!;
        transport.DropWrites();
        var pending = db.PingAsync();
        await transport.Dropped.WaitAsync(TimeSpan.FromSeconds(5));
        transport.CloseAsIfByPeer();

        var ex = await Assert.ThrowsAsync<RedisConnectionException>(() => pending);
        Assert.Equal(ConnectionFailureType.SocketClosed, ex.FailureType);

        var args = await failed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(ConnectionFailureType.SocketClosed, args.FailureType);
    }
}
