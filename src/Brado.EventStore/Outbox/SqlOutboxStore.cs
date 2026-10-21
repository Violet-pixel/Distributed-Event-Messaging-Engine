using Brado.EventStore.Dialects;
using Dapper;

namespace Brado.EventStore.Outbox;

public interface IMessagePublisher
{
    Task PublishAsync(string topic, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken = default);
}

public sealed record PendingOutboxMessage(long Id, Guid MessageId, string Topic, string Payload, string Headers, long Attempts, DateTimeOffset CreatedAt);

public sealed record OutboxStats(long Pending, long Dispatched, long Failing, DateTimeOffset? OldestPending);

public sealed record OutboxFailure(long Id, string Error, DateTimeOffset NextAttemptAt);

public interface IOutboxStore
{
    Task<IReadOnlyList<PendingOutboxMessage>> ClaimAsync(string workerId, int batchSize, TimeSpan lease, CancellationToken cancellationToken = default);

    Task MarkDispatchedAsync(IReadOnlyCollection<long> ids, CancellationToken cancellationToken = default);

    Task MarkFailedAsync(IReadOnlyCollection<OutboxFailure> failures, CancellationToken cancellationToken = default);

    Task<OutboxStats> GetStatsAsync(CancellationToken cancellationToken = default);

    Task<int> PurgeDispatchedAsync(DateTimeOffset before, CancellationToken cancellationToken = default);
}

public sealed class SqlOutboxStore(SqlDialect dialect) : IOutboxStore
{
    public async Task<IReadOnlyList<PendingOutboxMessage>> ClaimAsync(string workerId, int batchSize, TimeSpan lease, CancellationToken cancellationToken = default)
    {
        var now = DateTimeOffset.UtcNow;
        var sql = $"""
            UPDATE outbox SET locked_until = @LockedUntil, locked_by = @Worker
            WHERE id IN (
                SELECT id FROM outbox
                WHERE dispatched_at IS NULL
                  AND next_attempt_at <= @Now
                  AND (locked_until IS NULL OR locked_until < @Now)
                ORDER BY id
                LIMIT @BatchSize{dialect.ForUpdateSkipLocked})
            RETURNING id AS Id, message_id AS MessageId, topic AS Topic, {dialect.JsonText("payload")} AS Payload,
                      {dialect.JsonText("headers")} AS Headers, attempts AS Attempts, created_at AS CreatedAt
            """;

        await using var connection = await dialect.OpenAsync(cancellationToken);
        await using var transaction = await dialect.BeginWriteTransactionAsync(connection, cancellationToken);
        var rows = (await connection.QueryAsync<OutboxRow>(new CommandDefinition(sql,
            new { LockedUntil = now + lease, Worker = workerId, Now = now, BatchSize = batchSize }, transaction, cancellationToken: cancellationToken))).ToList();
        await transaction.CommitAsync(cancellationToken);

        return rows
            .OrderBy(r => r.Id)
            .Select(r => new PendingOutboxMessage(r.Id, Guid.Parse(r.MessageId), r.Topic, r.Payload, r.Headers, r.Attempts, DbValue.ToDateTimeOffset(r.CreatedAt)))
            .ToArray();
    }

    public async Task MarkDispatchedAsync(IReadOnlyCollection<long> ids, CancellationToken cancellationToken = default)
    {
        if (ids.Count == 0) return;
        var now = DateTimeOffset.UtcNow;
        await using var connection = await dialect.OpenAsync(cancellationToken);
        await using var transaction = await dialect.BeginWriteTransactionAsync(connection, cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition(
            "UPDATE outbox SET dispatched_at = @Now, locked_until = NULL, locked_by = NULL, last_error = NULL WHERE id = @Id",
            ids.Select(id => new { Id = id, Now = now }).ToArray(), transaction, cancellationToken: cancellationToken));
        await transaction.CommitAsync(cancellationToken);
    }

    public async Task MarkFailedAsync(IReadOnlyCollection<OutboxFailure> failures, CancellationToken cancellationToken = default)
    {
        if (failures.Count == 0) return;
        await using var connection = await dialect.OpenAsync(cancellationToken);
        await using var transaction = await dialect.BeginWriteTransactionAsync(connection, cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition("""
            UPDATE outbox
            SET attempts = attempts + 1, next_attempt_at = @NextAttemptAt, last_error = @Error, locked_until = NULL, locked_by = NULL
            WHERE id = @Id
            """,
            failures.Select(f => new { f.Id, NextAttemptAt = f.NextAttemptAt.ToUniversalTime(), Error = Truncate(f.Error, 2000) }).ToArray(),
            transaction, cancellationToken: cancellationToken));
        await transaction.CommitAsync(cancellationToken);
    }

    public async Task<OutboxStats> GetStatsAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await dialect.OpenAsync(cancellationToken);
        var row = await connection.QuerySingleAsync<StatsRow>(new CommandDefinition("""
            SELECT COALESCE(SUM(CASE WHEN dispatched_at IS NULL THEN 1 ELSE 0 END), 0)                  AS Pending,
                   COALESCE(SUM(CASE WHEN dispatched_at IS NOT NULL THEN 1 ELSE 0 END), 0)              AS Dispatched,
                   COALESCE(SUM(CASE WHEN dispatched_at IS NULL AND attempts > 0 THEN 1 ELSE 0 END), 0) AS Failing
            FROM outbox
            """, cancellationToken: cancellationToken));
        var oldest = await connection.ExecuteScalarAsync<object?>(new CommandDefinition(
            "SELECT MIN(created_at) FROM outbox WHERE dispatched_at IS NULL", cancellationToken: cancellationToken));
        return new OutboxStats(row.Pending, row.Dispatched, row.Failing, DbValue.ToNullableDateTimeOffset(oldest));
    }

    public async Task<int> PurgeDispatchedAsync(DateTimeOffset before, CancellationToken cancellationToken = default)
    {
        await using var connection = await dialect.OpenAsync(cancellationToken);
        return await connection.ExecuteAsync(new CommandDefinition(
            "DELETE FROM outbox WHERE dispatched_at IS NOT NULL AND dispatched_at < @Before",
            new { Before = before.ToUniversalTime() }, cancellationToken: cancellationToken));
    }

    private static string Truncate(string value, int max) => value.Length <= max ? value : value[..max];

    private sealed class OutboxRow
    {
        public long Id { get; set; }
        public string MessageId { get; set; } = "";
        public string Topic { get; set; } = "";
        public string Payload { get; set; } = "";
        public string Headers { get; set; } = "";
        public long Attempts { get; set; }
        public object? CreatedAt { get; set; }
    }

    private sealed class StatsRow
    {
        public long Pending { get; set; }
        public long Dispatched { get; set; }
        public long Failing { get; set; }
    }
}
