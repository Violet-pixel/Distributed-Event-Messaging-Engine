using System.Threading.Channels;
using Brado.Broker;
using Brado.EventStore.Dialects;
using Dapper;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Brado.EventStore.Journal;

public sealed class SqlMessageJournal : IMessageJournal, IAsyncDisposable
{
    private const int BatchSize = 512;

    private readonly SqlDialect _dialect;
    private readonly BrokerOptions _options;
    private readonly ILogger<SqlMessageJournal> _logger;
    private readonly Channel<(string Topic, StoredMessage Message)> _queue =
        Channel.CreateBounded<(string, StoredMessage)>(new BoundedChannelOptions(100_000) { SingleReader = true, FullMode = BoundedChannelFullMode.Wait });
    private readonly Task _writer;

    public SqlMessageJournal(SqlDialect dialect, IOptions<BrokerOptions> options, ILogger<SqlMessageJournal> logger)
    {
        _dialect = dialect;
        _options = options.Value;
        _logger = logger;
        _writer = Task.Run(WriteLoopAsync);
    }

    public ValueTask AppendAsync(string topic, StoredMessage message, CancellationToken cancellationToken = default) =>
        _queue.Writer.WriteAsync((topic, message), cancellationToken);

    public async Task<IReadOnlyList<string>> ListTopicsAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await _dialect.OpenAsync(cancellationToken);
        var topics = await connection.QueryAsync<string>(new CommandDefinition(
            "SELECT DISTINCT topic FROM broker_messages ORDER BY topic", cancellationToken: cancellationToken));
        return topics.ToArray();
    }

    public async Task<IReadOnlyList<StoredMessage>> ReadTailAsync(string topic, int max, CancellationToken cancellationToken = default)
    {
        await using var connection = await _dialect.OpenAsync(cancellationToken);
        var rows = await connection.QueryAsync<JournalRow>(new CommandDefinition("""
            SELECT msg_offset AS MsgOffset, payload AS Payload, stored_at AS StoredAt
            FROM broker_messages WHERE topic = @Topic
            ORDER BY msg_offset DESC
            LIMIT @Max
            """, new { Topic = topic, Max = max }, cancellationToken: cancellationToken));
        return rows.Reverse().Select(r => new StoredMessage(r.MsgOffset, r.Payload, DbValue.ToDateTimeOffset(r.StoredAt))).ToArray();
    }

    public async Task SaveOffsetsAsync(IReadOnlyList<GroupOffset> offsets, CancellationToken cancellationToken = default)
    {
        var now = DateTimeOffset.UtcNow;
        await using var connection = await _dialect.OpenAsync(cancellationToken);
        await using var transaction = await _dialect.BeginWriteTransactionAsync(connection, cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition("""
            INSERT INTO consumer_offsets (topic, group_name, committed_offset, updated_at)
            VALUES (@Topic, @Group, @CommittedOffset, @Now)
            ON CONFLICT (topic, group_name) DO UPDATE SET committed_offset = excluded.committed_offset, updated_at = excluded.updated_at
            """, offsets.Select(o => new { o.Topic, o.Group, o.CommittedOffset, Now = now }).ToArray(), transaction, cancellationToken: cancellationToken));
        await transaction.CommitAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<GroupOffset>> LoadOffsetsAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await _dialect.OpenAsync(cancellationToken);
        var rows = await connection.QueryAsync<OffsetRow>(new CommandDefinition(
            "SELECT topic AS Topic, group_name AS GroupName, committed_offset AS CommittedOffset FROM consumer_offsets",
            cancellationToken: cancellationToken));
        return rows.Select(r => new GroupOffset(r.Topic, r.GroupName, r.CommittedOffset)).ToArray();
    }

    private async Task WriteLoopAsync()
    {
        var batch = new List<(string Topic, StoredMessage Message)>(BatchSize);
        long batches = 0;

        while (await _queue.Reader.WaitToReadAsync())
        {
            while (batch.Count < BatchSize && _queue.Reader.TryRead(out var item)) batch.Add(item);

            for (var attempt = 1; ; attempt++)
            {
                try
                {
                    await WriteBatchAsync(batch);
                    break;
                }
                catch (Exception ex) when (attempt < 5)
                {
                    _logger.LogWarning(ex, "Falha ao gravar lote do journal (tentativa {Attempt}); tentando de novo", attempt);
                    await Task.Delay(TimeSpan.FromMilliseconds(200 * attempt));
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Lote de {Count} mensagens não pôde ser gravado no journal", batch.Count);
                    break;
                }
            }

            batch.Clear();
            if (++batches % 500 == 0) await TrimAsync();
        }
    }

    private async Task WriteBatchAsync(List<(string Topic, StoredMessage Message)> batch)
    {
        await using var connection = await _dialect.OpenAsync();
        await using var transaction = await _dialect.BeginWriteTransactionAsync(connection, CancellationToken.None);
        await connection.ExecuteAsync(new CommandDefinition("""
            INSERT INTO broker_messages (topic, msg_offset, payload, stored_at)
            VALUES (@Topic, @Offset, @Payload, @StoredAt)
            ON CONFLICT (topic, msg_offset) DO NOTHING
            """, batch.Select(b => new
            {
                b.Topic,
                b.Message.Offset,
                Payload = DbValue.ToArray(b.Message.Payload),
                StoredAt = b.Message.Timestamp.ToUniversalTime()
            }).ToArray(), transaction));
        await transaction.CommitAsync();
    }

    private async Task TrimAsync()
    {
        try
        {
            await using var connection = await _dialect.OpenAsync();
            await connection.ExecuteAsync(new CommandDefinition("""
                DELETE FROM broker_messages
                WHERE msg_offset < (SELECT MAX(b.msg_offset) FROM broker_messages b WHERE b.topic = broker_messages.topic) - @Retention
                """, new { Retention = (long)_options.RetainedMessagesPerTopic }));
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Falha ao aplicar retenção no journal");
        }
    }

    public async ValueTask DisposeAsync()
    {
        _queue.Writer.TryComplete();
        try { await _writer.WaitAsync(TimeSpan.FromSeconds(10)); } catch { }
    }

    private sealed class JournalRow
    {
        public long MsgOffset { get; set; }
        public byte[] Payload { get; set; } = [];
        public object? StoredAt { get; set; }
    }

    private sealed class OffsetRow
    {
        public string Topic { get; set; } = "";
        public string GroupName { get; set; } = "";
        public long CommittedOffset { get; set; }
    }
}
