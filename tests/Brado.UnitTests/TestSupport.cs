using System.Collections.Concurrent;
using Brado.Broker;
using Brado.Broker.Metrics;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace Brado.UnitTests;

internal static class Eventually
{
    public static async Task TrueAsync(Func<bool> condition, int timeoutMs = 5000)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (!condition())
        {
            if (DateTime.UtcNow > deadline) throw new TimeoutException("Condição não satisfeita a tempo.");
            await Task.Delay(10);
        }
    }
}

internal sealed class FakeTarget(long subscriptionId, int prefetch = 16) : IDeliveryTarget
{
    public ConcurrentQueue<(StoredMessage Message, int Attempt)> Received { get; } = new();
    public long SubscriptionId { get; } = subscriptionId;
    public int Prefetch { get; } = prefetch;
    public string Description => $"fake:{SubscriptionId}";

    public bool TryDeliver(string topic, string group, StoredMessage message, int attempt)
    {
        Received.Enqueue((message, attempt));
        return true;
    }
}

internal static class TestBroker
{
    public static MessageBroker Create(Action<BrokerOptions>? configure = null)
    {
        var options = new BrokerOptions { PersistMessages = false, MaxDeliveryAttempts = 3, AckTimeout = TimeSpan.FromSeconds(30) };
        configure?.Invoke(options);
        return new MessageBroker(new DefaultTopicFactory(options), new NullMessageJournal(), new BrokerMetrics(),
            Options.Create(options), NullLogger<MessageBroker>.Instance);
    }

    public static string TempFile(string name) =>
        Path.Combine(Path.GetTempPath(), $"brado-{name}-{Guid.NewGuid():N}.db");
}
