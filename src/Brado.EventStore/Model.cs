namespace Brado.EventStore;

public sealed class EventStoreOptions
{
    public const string Section = "EventStore";

    public string Provider { get; set; } = "Sqlite";
    public string ConnectionString { get; set; } = "Data Source=data/brado-events.db";

    public int SnapshotEvery { get; set; } = 50;
}

public static class ExpectedVersion
{
    public const long Any = -1;

    public const long NoStream = 0;
}

public sealed record EventData(Guid EventId, string EventType, string Payload, string Metadata, DateTimeOffset OccurredAt);

public sealed record RecordedEvent(
    long GlobalPosition,
    string StreamId,
    long StreamVersion,
    Guid EventId,
    string EventType,
    string Payload,
    string Metadata,
    DateTimeOffset OccurredAt);

public sealed record OutboxMessage(Guid MessageId, string Topic, string Payload, string Headers);

public sealed record StreamInfo(string StreamId, string StreamType, long Version, DateTimeOffset CreatedAt, DateTimeOffset UpdatedAt);

public sealed record Snapshot(string StreamId, long Version, string Payload, DateTimeOffset CreatedAt);

public sealed record EventStoreStats(long Events, long Streams, long LastPosition, IReadOnlyList<EventTypeCount> ByType);

public sealed record EventTypeCount(string EventType, long Count);

public sealed class ConcurrencyException(string streamId, long expected, long actual)
    : Exception($"Conflito de concorrência em '{streamId}': esperado v{expected}, atual v{actual}.")
{
    public string StreamId { get; } = streamId;
    public long Expected { get; } = expected;
    public long Actual { get; } = actual;
}
