using Brado.Broker.Server;

namespace Brado.Broker.Metrics;

public sealed record TopicSnapshot(
    string Name,
    long NextOffset,
    long FirstOffset,
    int Retained,
    IReadOnlyList<ConsumerGroupSnapshot> Groups);

public sealed record BrokerSnapshot(
    DateTimeOffset At,
    double UptimeSeconds,
    int Port,
    MetricsSnapshot Metrics,
    IReadOnlyList<TopicSnapshot> Topics,
    IReadOnlyList<ConnectionSnapshot> Connections);

public sealed class BrokerMonitor(MessageBroker broker, ConnectionRegistry connections)
{
    public BrokerSnapshot Snapshot()
    {
        var metrics = broker.Metrics.Snapshot();
        var topics = broker.Topics
            .OrderBy(t => t.Name, StringComparer.Ordinal)
            .Select(t => new TopicSnapshot(t.Name, t.NextOffset, t.FirstOffset, t.Retained,
                t.Groups.Select(g => g.Snapshot()).OrderBy(g => g.Name, StringComparer.Ordinal).ToArray()))
            .ToArray();

        var now = DateTimeOffset.UtcNow;
        return new BrokerSnapshot(now, Math.Round((now - metrics.StartedAt).TotalSeconds), broker.Options.Port,
            metrics, topics, connections.Snapshot());
    }

    public IReadOnlyList<StoredMessage> Peek(string topic, long fromOffset, int max) =>
        broker.TryGetTopic(topic, out var t) ? t.Read(fromOffset, Math.Clamp(max, 1, 500)) : [];
}
