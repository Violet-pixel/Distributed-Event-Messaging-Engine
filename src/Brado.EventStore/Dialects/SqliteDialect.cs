using System.Data.Common;
using Microsoft.Data.Sqlite;

namespace Brado.EventStore.Dialects;

public sealed class SqliteDialect : SqlDialect
{
    private readonly string _connectionString;

    public SqliteDialect(string connectionString)
    {
        var builder = new SqliteConnectionStringBuilder(connectionString);
        if (builder.DefaultTimeout < 30) builder.DefaultTimeout = 30;
        _connectionString = builder.ToString();

        var path = builder.DataSource;
        if (!string.IsNullOrWhiteSpace(path) && path != ":memory:" && !path.StartsWith("file:", StringComparison.OrdinalIgnoreCase))
        {
            var directory = Path.GetDirectoryName(Path.GetFullPath(path));
            if (!string.IsNullOrEmpty(directory)) Directory.CreateDirectory(directory);
        }
    }

    public override string Name => "Sqlite";

    public override string Json(string parameter) => parameter;

    public override string JsonText(string column) => column;

    public override string ForUpdate => string.Empty;

    public override string ForUpdateSkipLocked => string.Empty;

    public override string? GlobalAppendLock => null;

    public override bool IsUniqueViolation(Exception exception) =>
        exception is SqliteException { SqliteErrorCode: 19 };

    protected override DbConnection CreateConnection() => new SqliteConnection(_connectionString);

    protected override async Task OnOpenedAsync(DbConnection connection, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = "PRAGMA busy_timeout = 10000; PRAGMA foreign_keys = ON; PRAGMA synchronous = NORMAL;";
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public override Task<DbTransaction> BeginWriteTransactionAsync(DbConnection connection, CancellationToken cancellationToken) =>

        Task.FromResult<DbTransaction>(((SqliteConnection)connection).BeginTransaction(deferred: false));

    public override string SchemaScript => """
        PRAGMA journal_mode = WAL;

        CREATE TABLE IF NOT EXISTS streams (
            stream_id    TEXT    PRIMARY KEY,
            stream_type  TEXT    NOT NULL,
            version      INTEGER NOT NULL CHECK (version >= 0),
            created_at   TEXT    NOT NULL,
            updated_at   TEXT    NOT NULL
        );
        CREATE INDEX IF NOT EXISTS ix_streams_type_updated ON streams (stream_type, updated_at DESC);

        CREATE TABLE IF NOT EXISTS events (
            global_position INTEGER PRIMARY KEY AUTOINCREMENT,
            stream_id       TEXT    NOT NULL REFERENCES streams (stream_id),
            stream_version  INTEGER NOT NULL CHECK (stream_version > 0),
            event_id        TEXT    NOT NULL UNIQUE,
            event_type      TEXT    NOT NULL,
            payload         TEXT    NOT NULL CHECK (json_valid(payload)),
            metadata        TEXT    NOT NULL CHECK (json_valid(metadata)),
            occurred_at     TEXT    NOT NULL,
            recorded_at     TEXT    NOT NULL,
            UNIQUE (stream_id, stream_version)
        );
        CREATE INDEX IF NOT EXISTS ix_events_type ON events (event_type);

        CREATE TRIGGER IF NOT EXISTS trg_events_no_update BEFORE UPDATE ON events
        BEGIN SELECT RAISE(ABORT, 'events é append-only: UPDATE não é permitido'); END;
        CREATE TRIGGER IF NOT EXISTS trg_events_no_delete BEFORE DELETE ON events
        BEGIN SELECT RAISE(ABORT, 'events é append-only: DELETE não é permitido'); END;

        CREATE TABLE IF NOT EXISTS snapshots (
            stream_id  TEXT    PRIMARY KEY REFERENCES streams (stream_id),
            version    INTEGER NOT NULL,
            payload    TEXT    NOT NULL,
            created_at TEXT    NOT NULL
        );

        CREATE TABLE IF NOT EXISTS outbox (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id      TEXT    NOT NULL UNIQUE,
            topic           TEXT    NOT NULL,
            payload         TEXT    NOT NULL,
            headers         TEXT    NOT NULL,
            created_at      TEXT    NOT NULL,
            attempts        INTEGER NOT NULL DEFAULT 0,
            next_attempt_at TEXT    NOT NULL,
            locked_until    TEXT    NULL,
            locked_by       TEXT    NULL,
            dispatched_at   TEXT    NULL,
            last_error      TEXT    NULL
        );
        CREATE INDEX IF NOT EXISTS ix_outbox_pending ON outbox (next_attempt_at, id) WHERE dispatched_at IS NULL;
        CREATE INDEX IF NOT EXISTS ix_outbox_dispatched ON outbox (dispatched_at) WHERE dispatched_at IS NOT NULL;

        CREATE TABLE IF NOT EXISTS saga_instances (
            saga_id        TEXT    PRIMARY KEY,
            saga_type      TEXT    NOT NULL,
            correlation_id TEXT    NOT NULL,
            state          TEXT    NOT NULL,
            data           TEXT    NOT NULL,
            version        INTEGER NOT NULL,
            created_at     TEXT    NOT NULL,
            updated_at     TEXT    NOT NULL,
            UNIQUE (saga_type, correlation_id)
        );
        CREATE INDEX IF NOT EXISTS ix_saga_updated ON saga_instances (updated_at DESC);

        CREATE TABLE IF NOT EXISTS processed_messages (
            consumer     TEXT NOT NULL,
            message_id   TEXT NOT NULL,
            processed_at TEXT NOT NULL,
            PRIMARY KEY (consumer, message_id)
        );

        CREATE TABLE IF NOT EXISTS broker_messages (
            topic      TEXT    NOT NULL,
            msg_offset INTEGER NOT NULL,
            payload    BLOB    NOT NULL,
            stored_at  TEXT    NOT NULL,
            PRIMARY KEY (topic, msg_offset)
        );

        CREATE TABLE IF NOT EXISTS consumer_offsets (
            topic            TEXT    NOT NULL,
            group_name       TEXT    NOT NULL,
            committed_offset INTEGER NOT NULL,
            updated_at       TEXT    NOT NULL,
            PRIMARY KEY (topic, group_name)
        );
        """;
}

public static class SqlDialectFactory
{
    public static SqlDialect Create(EventStoreOptions options) => options.Provider.Trim().ToLowerInvariant() switch
    {
        "postgres" or "postgresql" or "npgsql" => new PostgresDialect(options.ConnectionString),
        "sqlite" => new SqliteDialect(options.ConnectionString),
        var other => throw new NotSupportedException($"Provider de event store desconhecido: '{other}'. Use Sqlite ou Postgres.")
    };
}
