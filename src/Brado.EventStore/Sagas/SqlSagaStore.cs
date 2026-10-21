using Brado.EventStore.Dialects;
using Dapper;

namespace Brado.EventStore.Sagas;

public sealed record SagaInstance(
    Guid SagaId,
    string SagaType,
    string CorrelationId,
    string State,
    string Data,
    long Version,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt);

public interface ISagaStore
{
    Task<SagaInstance?> FindAsync(string sagaType, string correlationId, CancellationToken cancellationToken = default);
    Task SaveAsync(SagaInstance saga, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<SagaInstance>> ListRecentAsync(int take, CancellationToken cancellationToken = default);
    Task<IReadOnlyDictionary<string, long>> CountByStateAsync(CancellationToken cancellationToken = default);
}

public interface IInboxStore
{
    Task<bool> HasProcessedAsync(string consumer, Guid messageId, CancellationToken cancellationToken = default);
    Task MarkProcessedAsync(string consumer, Guid messageId, CancellationToken cancellationToken = default);
}

public sealed class SqlSagaStore(SqlDialect dialect) : ISagaStore
{
    private string Columns => $"""
        SELECT saga_id AS SagaId, saga_type AS SagaType, correlation_id AS CorrelationId, state AS State,
               {dialect.JsonText("data")} AS Data, version AS Version, created_at AS CreatedAt, updated_at AS UpdatedAt
        FROM saga_instances
        """;

    public async Task<SagaInstance?> FindAsync(string sagaType, string correlationId, CancellationToken cancellationToken = default)
    {
        await using var connection = await dialect.OpenAsync(cancellationToken);
        var row = await connection.QuerySingleOrDefaultAsync<SagaRow>(new CommandDefinition(
            $"{Columns} WHERE saga_type = @SagaType AND correlation_id = @CorrelationId",
            new { SagaType = sagaType, CorrelationId = correlationId }, cancellationToken: cancellationToken));
        return row?.ToInstance();
    }

    public async Task SaveAsync(SagaInstance saga, CancellationToken cancellationToken = default)
    {
        await using var connection = await dialect.OpenAsync(cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition($"""
            INSERT INTO saga_instances (saga_id, saga_type, correlation_id, state, data, version, created_at, updated_at)
            VALUES (@SagaId, @SagaType, @CorrelationId, @State, {dialect.Json("@Data")}, 1, @CreatedAt, @UpdatedAt)
            ON CONFLICT (saga_type, correlation_id) DO UPDATE
            SET state = excluded.state, data = excluded.data, version = saga_instances.version + 1, updated_at = excluded.updated_at
            """,
            new
            {
                SagaId = saga.SagaId.ToString(),
                saga.SagaType,
                saga.CorrelationId,
                saga.State,
                saga.Data,
                CreatedAt = saga.CreatedAt.ToUniversalTime(),
                UpdatedAt = DateTimeOffset.UtcNow
            }, cancellationToken: cancellationToken));
    }

    public async Task<IReadOnlyList<SagaInstance>> ListRecentAsync(int take, CancellationToken cancellationToken = default)
    {
        await using var connection = await dialect.OpenAsync(cancellationToken);
        var rows = await connection.QueryAsync<SagaRow>(new CommandDefinition(
            $"{Columns} ORDER BY updated_at DESC LIMIT @Take", new { Take = Math.Clamp(take, 1, 200) }, cancellationToken: cancellationToken));
        return rows.Select(r => r.ToInstance()).ToArray();
    }

    public async Task<IReadOnlyDictionary<string, long>> CountByStateAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await dialect.OpenAsync(cancellationToken);
        var rows = await connection.QueryAsync<(string State, long Total)>(new CommandDefinition(
            "SELECT state, COUNT(*) FROM saga_instances GROUP BY state", cancellationToken: cancellationToken));
        return rows.ToDictionary(r => r.State, r => r.Total);
    }

    private sealed class SagaRow
    {
        public string SagaId { get; set; } = "";
        public string SagaType { get; set; } = "";
        public string CorrelationId { get; set; } = "";
        public string State { get; set; } = "";
        public string Data { get; set; } = "{}";
        public long Version { get; set; }
        public object? CreatedAt { get; set; }
        public object? UpdatedAt { get; set; }

        public SagaInstance ToInstance() => new(Guid.Parse(SagaId), SagaType, CorrelationId, State, Data, Version,
            DbValue.ToDateTimeOffset(CreatedAt), DbValue.ToDateTimeOffset(UpdatedAt));
    }
}

public sealed class SqlInboxStore(SqlDialect dialect) : IInboxStore
{
    public async Task<bool> HasProcessedAsync(string consumer, Guid messageId, CancellationToken cancellationToken = default)
    {
        await using var connection = await dialect.OpenAsync(cancellationToken);
        var count = await connection.ExecuteScalarAsync<long>(new CommandDefinition(
            "SELECT COUNT(*) FROM processed_messages WHERE consumer = @Consumer AND message_id = @MessageId",
            new { Consumer = consumer, MessageId = messageId.ToString() }, cancellationToken: cancellationToken));
        return count > 0;
    }

    public async Task MarkProcessedAsync(string consumer, Guid messageId, CancellationToken cancellationToken = default)
    {
        await using var connection = await dialect.OpenAsync(cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition("""
            INSERT INTO processed_messages (consumer, message_id, processed_at)
            VALUES (@Consumer, @MessageId, @Now)
            ON CONFLICT (consumer, message_id) DO NOTHING
            """, new { Consumer = consumer, MessageId = messageId.ToString(), Now = DateTimeOffset.UtcNow }, cancellationToken: cancellationToken));
    }
}
