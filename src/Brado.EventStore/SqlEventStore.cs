using System.Data.Common;
using Brado.EventStore.Dialects;
using Dapper;

namespace Brado.EventStore;

public interface IEventStore
{
    Task<long> AppendAsync(string streamId, string streamType, long expectedVersion, IReadOnlyList<EventData> events,
        IReadOnlyList<OutboxMessage>? outbox = null, CancellationToken cancellationToken = default);

    Task<IReadOnlyList<RecordedEvent>> ReadStreamAsync(string streamId, long afterVersion = 0, CancellationToken cancellationToken = default);

    Task<IReadOnlyList<RecordedEvent>> ReadAllAsync(long afterPosition, int maxCount, CancellationToken cancellationToken = default);

    Task<IReadOnlyList<StreamInfo>> ListStreamsAsync(int skip, int take, string? search = null, CancellationToken cancellationToken = default);

    Task<EventStoreStats> GetStatsAsync(CancellationToken cancellationToken = default);

    Task<Snapshot?> LoadSnapshotAsync(string streamId, CancellationToken cancellationToken = default);

    Task SaveSnapshotAsync(Snapshot snapshot, CancellationToken cancellationToken = default);
}

public sealed class SqlEventStore : IEventStore
{
    private readonly SqlDialect _dialect;
    private readonly string _insertEvent;
    private readonly string _insertOutbox;
    private readonly string _selectColumns;
    private readonly string _upsertSnapshot;

    private const string UpsertStream = """
        INSERT INTO streams (stream_id, stream_type, version, created_at, updated_at)
        VALUES (@StreamId, @StreamType, @Version, @Now, @Now)
        ON CONFLICT (stream_id) DO UPDATE SET version = excluded.version, updated_at = excluded.updated_at
        """;

    public SqlEventStore(SqlDialect dialect)
    {
        _dialect = dialect;
        _insertEvent = $"""
            INSERT INTO events (stream_id, stream_version, event_id, event_type, payload, metadata, occurred_at, recorded_at)
            VALUES (@StreamId, @StreamVersion, @EventId, @EventType, {dialect.Json("@Payload")}, {dialect.Json("@Metadata")}, @OccurredAt, @RecordedAt)
            """;
        _insertOutbox = $"""
            INSERT INTO outbox (message_id, topic, payload, headers, created_at, attempts, next_attempt_at)
            VALUES (@MessageId, @Topic, {dialect.Json("@Payload")}, {dialect.Json("@Headers")}, @Now, 0, @Now)
            """;
        _selectColumns = $"""
            SELECT global_position AS GlobalPosition, stream_id AS StreamId, stream_version AS StreamVersion,
                   event_id AS EventId, event_type AS EventType, {dialect.JsonText("payload")} AS Payload,
                   {dialect.JsonText("metadata")} AS Metadata, occurred_at AS OccurredAt
            FROM events
            """;
        _upsertSnapshot = $"""
            INSERT INTO snapshots (stream_id, version, payload, created_at)
            VALUES (@StreamId, @Version, {dialect.Json("@Payload")}, @CreatedAt)
            ON CONFLICT (stream_id) DO UPDATE SET version = excluded.version, payload = excluded.payload, created_at = excluded.created_at
            """;
    }

    public async Task<long> AppendAsync(string streamId, string streamType, long expectedVersion, IReadOnlyList<EventData> events,
        IReadOnlyList<OutboxMessage>? outbox = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId);
        if (events.Count == 0) throw new ArgumentException("Nenhum evento para gravar.", nameof(events));

        await using var connection = await _dialect.OpenAsync(cancellationToken);
        await using var transaction = await _dialect.BeginWriteTransactionAsync(connection, cancellationToken);
        try
        {
            if (_dialect.GlobalAppendLock is { } globalLock)
                await connection.ExecuteAsync(new CommandDefinition(globalLock, transaction: transaction, cancellationToken: cancellationToken));

            var current = await connection.ExecuteScalarAsync<long?>(new CommandDefinition(
                $"SELECT version FROM streams WHERE stream_id = @StreamId{_dialect.ForUpdate}",
                new { StreamId = streamId }, transaction, cancellationToken: cancellationToken)) ?? 0L;

            if (expectedVersion != ExpectedVersion.Any && current != expectedVersion)
                throw new ConcurrencyException(streamId, expectedVersion, current);

            var now = DateTimeOffset.UtcNow;
            var newVersion = current + events.Count;

            await connection.ExecuteAsync(new CommandDefinition(UpsertStream,
                new { StreamId = streamId, StreamType = streamType, Version = newVersion, Now = now }, transaction, cancellationToken: cancellationToken));

            var rows = events.Select((e, i) => new
            {
                StreamId = streamId,
                StreamVersion = current + i + 1,
                EventId = e.EventId.ToString(),
                e.EventType,
                e.Payload,
                e.Metadata,
                OccurredAt = e.OccurredAt.ToUniversalTime(),
                RecordedAt = now
            }).ToArray();
            await connection.ExecuteAsync(new CommandDefinition(_insertEvent, rows, transaction, cancellationToken: cancellationToken));

            if (outbox is { Count: > 0 })
            {
                var messages = outbox.Select(m => new { MessageId = m.MessageId.ToString(), m.Topic, m.Payload, m.Headers, Now = now }).ToArray();
                await connection.ExecuteAsync(new CommandDefinition(_insertOutbox, messages, transaction, cancellationToken: cancellationToken));
            }

            await transaction.CommitAsync(cancellationToken);
            return newVersion;
        }
        catch (DbException ex) when (_dialect.IsUniqueViolation(ex))
        {
            throw new ConcurrencyException(streamId, expectedVersion, -1);
        }
    }

    public async Task<IReadOnlyList<RecordedEvent>> ReadStreamAsync(string streamId, long afterVersion = 0, CancellationToken cancellationToken = default)
    {
        await using var connection = await _dialect.OpenAsync(cancellationToken);
        var rows = await connection.QueryAsync<EventRow>(new CommandDefinition(
            $"{_selectColumns} WHERE stream_id = @StreamId AND stream_version > @After ORDER BY stream_version",
            new { StreamId = streamId, After = afterVersion }, cancellationToken: cancellationToken));
        return rows.Select(r => r.ToRecorded()).ToArray();
    }

    public async Task<IReadOnlyList<RecordedEvent>> ReadAllAsync(long afterPosition, int maxCount, CancellationToken cancellationToken = default)
    {
        await using var connection = await _dialect.OpenAsync(cancellationToken);
        var rows = await connection.QueryAsync<EventRow>(new CommandDefinition(
            $"{_selectColumns} WHERE global_position > @After ORDER BY global_position LIMIT @Max",
            new { After = afterPosition, Max = Math.Clamp(maxCount, 1, 5000) }, cancellationToken: cancellationToken));
        return rows.Select(r => r.ToRecorded()).ToArray();
    }

    public async Task<IReadOnlyList<StreamInfo>> ListStreamsAsync(int skip, int take, string? search = null, CancellationToken cancellationToken = default)
    {
        await using var connection = await _dialect.OpenAsync(cancellationToken);
        var rows = await connection.QueryAsync<StreamRow>(new CommandDefinition("""
            SELECT stream_id AS StreamId, stream_type AS StreamType, version AS Version, created_at AS CreatedAt, updated_at AS UpdatedAt
            FROM streams
            WHERE (@Search IS NULL OR stream_id LIKE @Search)
            ORDER BY updated_at DESC
            LIMIT @Take OFFSET @Skip
            """,
            new { Search = string.IsNullOrWhiteSpace(search) ? null : $"%{search.Trim()}%", Take = Math.Clamp(take, 1, 500), Skip = Math.Max(0, skip) },
            cancellationToken: cancellationToken));
        return rows.Select(r => new StreamInfo(r.StreamId, r.StreamType, r.Version, DbValue.ToDateTimeOffset(r.CreatedAt), DbValue.ToDateTimeOffset(r.UpdatedAt))).ToArray();
    }

    public async Task<EventStoreStats> GetStatsAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await _dialect.OpenAsync(cancellationToken);
        var events = await connection.ExecuteScalarAsync<long>(new CommandDefinition("SELECT COUNT(*) FROM events", cancellationToken: cancellationToken));
        var streams = await connection.ExecuteScalarAsync<long>(new CommandDefinition("SELECT COUNT(*) FROM streams", cancellationToken: cancellationToken));
        var last = await connection.ExecuteScalarAsync<long?>(new CommandDefinition("SELECT MAX(global_position) FROM events", cancellationToken: cancellationToken)) ?? 0;
        var byType = await connection.QueryAsync<TypeCountRow>(new CommandDefinition(
            "SELECT event_type AS EventType, COUNT(*) AS Total FROM events GROUP BY event_type ORDER BY COUNT(*) DESC",
            cancellationToken: cancellationToken));
        return new EventStoreStats(events, streams, last, byType.Select(t => new EventTypeCount(t.EventType, t.Total)).ToArray());
    }

    public async Task<Snapshot?> LoadSnapshotAsync(string streamId, CancellationToken cancellationToken = default)
    {
        await using var connection = await _dialect.OpenAsync(cancellationToken);
        var row = await connection.QuerySingleOrDefaultAsync<SnapshotRow>(new CommandDefinition(
            $"SELECT stream_id AS StreamId, version AS Version, {_dialect.JsonText("payload")} AS Payload, created_at AS CreatedAt FROM snapshots WHERE stream_id = @StreamId",
            new { StreamId = streamId }, cancellationToken: cancellationToken));
        return row is null ? null : new Snapshot(row.StreamId, row.Version, row.Payload, DbValue.ToDateTimeOffset(row.CreatedAt));
    }

    public async Task SaveSnapshotAsync(Snapshot snapshot, CancellationToken cancellationToken = default)
    {
        await using var connection = await _dialect.OpenAsync(cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition(_upsertSnapshot,
            new { snapshot.StreamId, snapshot.Version, snapshot.Payload, CreatedAt = snapshot.CreatedAt.ToUniversalTime() },
            cancellationToken: cancellationToken));
    }

    private sealed class EventRow
    {
        public long GlobalPosition { get; set; }
        public string StreamId { get; set; } = "";
        public long StreamVersion { get; set; }
        public string EventId { get; set; } = "";
        public string EventType { get; set; } = "";
        public string Payload { get; set; } = "";
        public string Metadata { get; set; } = "";
        public object? OccurredAt { get; set; }

        public RecordedEvent ToRecorded() => new(GlobalPosition, StreamId, StreamVersion, Guid.Parse(EventId), EventType, Payload, Metadata,
            DbValue.ToDateTimeOffset(OccurredAt));
    }

    private sealed class StreamRow
    {
        public string StreamId { get; set; } = "";
        public string StreamType { get; set; } = "";
        public long Version { get; set; }
        public object? CreatedAt { get; set; }
        public object? UpdatedAt { get; set; }
    }

    private sealed class SnapshotRow
    {
        public string StreamId { get; set; } = "";
        public long Version { get; set; }
        public string Payload { get; set; } = "";
        public object? CreatedAt { get; set; }
    }

    private sealed class TypeCountRow
    {
        public string EventType { get; set; } = "";
        public long Total { get; set; }
    }
}
