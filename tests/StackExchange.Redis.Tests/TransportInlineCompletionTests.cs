using System.Threading.Tasks;
using Xunit;

namespace StackExchange.Redis.Tests;

[RunPerProtocol]
public class TransportInlineCompletionTests(ITestOutputHelper output)
{
    [Fact]
    public async Task AwaitedCommandResumesInsideTheDeliveryWhenTheTunnelCompletesInline()
        => Assert.True(await ResumesInsideDeliveryAsync(completesInline: true));

    [Fact]
    public async Task AwaitedCommandResumesOffTheTransportByDefault()
        => Assert.False(await ResumesInsideDeliveryAsync(completesInline: false));

    private async Task<bool> ResumesInsideDeliveryAsync(bool completesInline)
    {
        using var server = new InProcessTestServer(output);
        var config = server.GetClientConfig(withPubSub: false, defaultOnly: true);
        config.Tunnel = new StreamTransportTunnel(server.Tunnel, completesInline);

        await using var conn = await ConnectionMultiplexer.ConnectAsync(config);
        var db = conn.GetDatabase();
        await db.StringSetAsync("key", "value");

        var value = await db.StringGetAsync("key");
        bool inside = StreamTransport.IsDelivering;
        Assert.Equal("value", value);

        await Task.Yield(); // off the transport's thread before tearing down
        return inside;
    }
}
