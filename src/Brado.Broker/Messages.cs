namespace Brado.Broker;

public sealed record StoredMessage(long Offset, ReadOnlyMemory<byte> Payload, DateTimeOffset Timestamp);

public sealed record Delivery(
    string Topic,
    string Group,
    long SubscriptionId,
    long Offset,
    ReadOnlyMemory<byte> Payload,
    int Attempt,
    DateTimeOffset Timestamp)
{
    public bool Redelivered => Attempt > 1;
}

public interface IDeliveryTarget
{
    long SubscriptionId { get; }
    int Prefetch { get; }
    string Description { get; }

    bool TryDeliver(string topic, string group, StoredMessage message, int attempt);
}

public interface IMessageJournal
{
    ValueTask AppendAsync(string topic, StoredMessage message, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<string>> ListTopicsAsync(CancellationToken cancellationToken = default);
    Task<IReadOnlyList<StoredMessage>> ReadTailAsync(string topic, int max, CancellationToken cancellationToken = default);
    Task SaveOffsetsAsync(IReadOnlyList<GroupOffset> offsets, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<GroupOffset>> LoadOffsetsAsync(CancellationToken cancellationToken = default);
}

public sealed record GroupOffset(string Topic, string Group, long CommittedOffset);

public sealed class NullMessageJournal : IMessageJournal
{
    public ValueTask AppendAsync(string topic, StoredMessage message, CancellationToken cancellationToken = default) => ValueTask.CompletedTask;
    public Task<IReadOnlyList<string>> ListTopicsAsync(CancellationToken cancellationToken = default) => Task.FromResult<IReadOnlyList<string>>([]);
    public Task<IReadOnlyList<StoredMessage>> ReadTailAsync(string topic, int max, CancellationToken cancellationToken = default) => Task.FromResult<IReadOnlyList<StoredMessage>>([]);
    public Task SaveOffsetsAsync(IReadOnlyList<GroupOffset> offsets, CancellationToken cancellationToken = default) => Task.CompletedTask;
    public Task<IReadOnlyList<GroupOffset>> LoadOffsetsAsync(CancellationToken cancellationToken = default) => Task.FromResult<IReadOnlyList<GroupOffset>>([]);
}
