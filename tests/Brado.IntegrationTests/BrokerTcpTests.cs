using System.Collections.Concurrent;
using System.Text;
using Brado.Broker;
using Brado.Broker.Server;
using Brado.Client;
using Brado.Protocol;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Brado.IntegrationTests;

public sealed class BrokerTcpTests : IAsyncLifetime
{
    private ServiceProvider _services = null!;
    private BrokerServer _server = null!;
    private int _port;

    public async ValueTask InitializeAsync()
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Broker:Host"] = "127.0.0.1",
            ["Broker:Port"] = "0",
            ["Broker:PersistMessages"] = "false",
            ["Broker:MaxDeliveryAttempts"] = "3"
        }).Build();

        _services = new ServiceCollection().AddLogging().AddBradoBroker(configuration).BuildServiceProvider();
        _server = _services.GetRequiredService<BrokerServer>();
        await _server.StartAsync(CancellationToken.None);
        _port = (await _server.Started.WaitAsync(TimeSpan.FromSeconds(10))).Port;
    }

    public async ValueTask DisposeAsync()
    {
        await _server.StopAsync(CancellationToken.None);
        await _services.DisposeAsync();
    }

    private Task<BradoConnection> Connect(string name) => BradoConnection.ConnectAsync("127.0.0.1", _port, name);

    private static async Task Until(Func<bool> condition, int timeoutMs = 10_000)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (!condition())
        {
            if (DateTime.UtcNow > deadline) throw new TimeoutException();
            await Task.Delay(20);
        }
    }

    [Fact]
    public async Task Publish_and_subscribe_over_tcp()
    {
        await using var consumer = await Connect("consumer");
        await using var producer = await Connect("producer");
        var received = new ConcurrentBag<string>();
        await consumer.SubscribeAsync("pedidos.criados", "faturamento", (m, _) => { received.Add(m.AsString()); return Task.CompletedTask; },
            startOffset: StartPosition.Earliest);

        for (var i = 0; i < 100; i++) await producer.PublishAsync("pedidos.criados", $"pedido-{i}");

        await Until(() => received.Count == 100);
        Assert.Equal(100, received.Distinct().Count());
    }

    [Fact]
    public async Task Publisher_receives_increasing_offsets()
    {
        await using var producer = await Connect("producer");
        var first = await producer.PublishAsync("offsets", "a");
        var second = await producer.PublishAsync("offsets", "b");
        Assert.Equal(first + 1, second);
    }

    [Fact]
    public async Task Consumers_in_the_same_group_share_the_load()
    {
        await using var a = await Connect("worker-a");
        await using var b = await Connect("worker-b");
        await using var producer = await Connect("producer");
        long countA = 0, countB = 0;
        await a.SubscribeAsync("jobs", "workers", async (_, ct) => { Interlocked.Increment(ref countA); await Task.Delay(2, ct); }, prefetch: 4, startOffset: StartPosition.Earliest);
        await b.SubscribeAsync("jobs", "workers", async (_, ct) => { Interlocked.Increment(ref countB); await Task.Delay(2, ct); }, prefetch: 4, startOffset: StartPosition.Earliest);

        await Task.WhenAll(Enumerable.Range(0, 200).Select(i => producer.PublishAsync("jobs", Encoding.UTF8.GetBytes($"job-{i}"))));

        await Until(() => Interlocked.Read(ref countA) + Interlocked.Read(ref countB) == 200);
        Assert.True(countA > 20 && countB > 20, $"distribuição desigual: {countA}/{countB}");
    }

    [Fact]
    public async Task Failing_handler_gets_redelivered_message()
    {
        await using var consumer = await Connect("flaky");
        await using var producer = await Connect("producer");
        var attempts = 0;
        var redelivered = false;
        await consumer.SubscribeAsync("instavel", "g", (m, _) =>
        {
            if (Interlocked.Increment(ref attempts) == 1) throw new InvalidOperationException("falha transitória");
            redelivered = m.Redelivered;
            return Task.CompletedTask;
        }, startOffset: StartPosition.Earliest);

        await producer.PublishAsync("instavel", "x");

        await Until(() => Volatile.Read(ref attempts) >= 2);
        await Until(() => redelivered);
    }

    [Fact]
    public async Task Messages_of_a_disconnected_consumer_go_to_the_remaining_member()
    {
        var survivor = await Connect("survivor");
        var leaver = await Connect("leaver");
        await using var producer = await Connect("producer");
        var received = new ConcurrentDictionary<long, bool>();
        var block = new TaskCompletionSource();

        await leaver.SubscribeAsync("handoff", "g", async (_, ct) => await block.Task.WaitAsync(ct), prefetch: 10, startOffset: StartPosition.Earliest);
        for (var i = 0; i < 5; i++) await producer.PublishAsync("handoff", $"m{i}");
        await Task.Delay(300);

        await survivor.SubscribeAsync("handoff", "g", (m, _) => { received[m.Offset] = true; return Task.CompletedTask; }, startOffset: StartPosition.Earliest);
        await leaver.DisposeAsync();

        await Until(() => received.Count == 5);
        block.TrySetResult();
        await survivor.DisposeAsync();
    }

    [Fact]
    public async Task Invalid_topic_returns_protocol_error()
    {
        await using var producer = await Connect("producer");
        await Assert.ThrowsAnyAsync<Exception>(() => producer.PublishAsync("tópico inválido", "x"));
        Assert.True((await producer.PingAsync()) < TimeSpan.FromSeconds(5));
    }
}
