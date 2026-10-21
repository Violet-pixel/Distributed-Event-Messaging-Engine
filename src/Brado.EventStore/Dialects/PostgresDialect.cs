using System.Data.Common;
using Npgsql;

namespace Brado.EventStore.Dialects;

public sealed class PostgresDialect(string connectionString) : SqlDialect
{
    private readonly NpgsqlDataSource _dataSource = NpgsqlDataSource.Create(connectionString);

    public override string Name => "Postgres";

    public override string Json(string parameter) => $"CAST({parameter} AS jsonb)";

    public override string JsonText(string column) => $"{column}::text";

    public override string ForUpdate => " FOR UPDATE";

    public override string ForUpdateSkipLocked => " FOR UPDATE SKIP LOCKED";

    public override string GlobalAppendLock => "SELECT pg_advisory_xact_lock(74201)";

    public override bool IsUniqueViolation(Exception exception) =>
        exception is PostgresException { SqlState: PostgresErrorCodes.UniqueViolation };

    protected override DbConnection CreateConnection() => _dataSource.CreateConnection();

    public override ValueTask DisposeAsync() => _dataSource.DisposeAsync();

    public override string SchemaScript => """
        CREATE TABLE IF NOT EXISTS streams (
            stream_id    TEXT        PRIMARY KEY,
            stream_type  TEXT        NOT NULL,
            version      BIGINT      NOT NULL CHECK (version >= 0),
            created_at   TIMESTAMPTZ NOT NULL,
            updated_at   TIMESTAMPTZ NOT NULL
        );
        CREATE INDEX IF NOT EXISTS ix_streams_type_updated ON streams (stream_type, updated_at DESC);

        CREATE TABLE IF NOT EXISTS events (
            global_position BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            stream_id       TEXT        NOT NULL REFERENCES streams (stream_id),
            stream_version  BIGINT      NOT NULL CHECK (stream_version > 0),
            event_id        TEXT        NOT NULL,
            event_type      TEXT        NOT NULL,
            payload         JSONB       NOT NULL,
            metadata        JSONB       NOT NULL,
            occurred_at     TIMESTAMPTZ NOT NULL,
            recorded_at     TIMESTAMPTZ NOT NULL,
            CONSTRAINT uq_events_stream_version UNIQUE (stream_id, stream_version),
            CONSTRAINT uq_events_event_id UNIQUE (event_id)
        );
        CREATE INDEX IF NOT EXISTS ix_events_type ON events (event_type);

        CREATE OR REPLACE FUNCTION brado_events_are_immutable() RETURNS trigger AS $$
        BEGIN
            RAISE EXCEPTION 'events é append-only: UPDATE/DELETE não são permitidos';
        END;
        $$ LANGUAGE plpgsql;
        DROP TRIGGER IF EXISTS trg_events_immutable ON events;
        CREATE TRIGGER trg_events_immutable BEFORE UPDATE OR DELETE ON events
            FOR EACH ROW EXECUTE FUNCTION brado_events_are_immutable();

        CREATE TABLE IF NOT EXISTS snapshots (
            stream_id  TEXT        PRIMARY KEY REFERENCES streams (stream_id),
            version    BIGINT      NOT NULL,
            payload    JSONB       NOT NULL,
            created_at TIMESTAMPTZ NOT NULL
        );

        CREATE TABLE IF NOT EXISTS outbox (
            id              BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            message_id      TEXT        NOT NULL UNIQUE,
            topic           TEXT        NOT NULL,
            payload         JSONB       NOT NULL,
            headers         JSONB       NOT NULL,
            created_at      TIMESTAMPTZ NOT NULL,
            attempts        BIGINT      NOT NULL DEFAULT 0,
            next_attempt_at TIMESTAMPTZ NOT NULL,
            locked_until    TIMESTAMPTZ NULL,
            locked_by       TEXT        NULL,
            dispatched_at   TIMESTAMPTZ NULL,
            last_error      TEXT        NULL
        );
        CREATE INDEX IF NOT EXISTS ix_outbox_pending ON outbox (next_attempt_at, id) WHERE dispatched_at IS NULL;
        CREATE INDEX IF NOT EXISTS ix_outbox_dispatched ON outbox (dispatched_at) WHERE dispatched_at IS NOT NULL;

        CREATE TABLE IF NOT EXISTS saga_instances (
            saga_id        TEXT        PRIMARY KEY,
            saga_type      TEXT        NOT NULL,
            correlation_id TEXT        NOT NULL,
            state          TEXT        NOT NULL,
            data           JSONB       NOT NULL,
            version        BIGINT      NOT NULL,
            created_at     TIMESTAMPTZ NOT NULL,
            updated_at     TIMESTAMPTZ NOT NULL,
            CONSTRAINT uq_saga_correlation UNIQUE (saga_type, correlation_id)
        );
        CREATE INDEX IF NOT EXISTS ix_saga_updated ON saga_instances (updated_at DESC);

        CREATE TABLE IF NOT EXISTS processed_messages (
            consumer     TEXT        NOT NULL,
            message_id   TEXT        NOT NULL,
            processed_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (consumer, message_id)
        );

        CREATE TABLE IF NOT EXISTS broker_messages (
            topic      TEXT        NOT NULL,
            msg_offset BIGINT      NOT NULL,
            payload    BYTEA       NOT NULL,
            stored_at  TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (topic, msg_offset)
        );

        CREATE TABLE IF NOT EXISTS consumer_offsets (
            topic            TEXT        NOT NULL,
            group_name       TEXT        NOT NULL,
            committed_offset BIGINT      NOT NULL,
            updated_at       TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (topic, group_name)
        );
        """;
}
