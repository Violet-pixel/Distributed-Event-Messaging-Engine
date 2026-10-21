using System.Data.Common;
using System.Globalization;

namespace Brado.EventStore.Dialects;

public abstract class SqlDialect : IAsyncDisposable
{
    public abstract string Name { get; }

    public abstract string SchemaScript { get; }

    public abstract string Json(string parameter);

    public abstract string JsonText(string column);

    public abstract string ForUpdate { get; }

    public abstract string ForUpdateSkipLocked { get; }

    public abstract string? GlobalAppendLock { get; }

    public abstract bool IsUniqueViolation(Exception exception);

    protected abstract DbConnection CreateConnection();

    public async Task<DbConnection> OpenAsync(CancellationToken cancellationToken = default)
    {
        var connection = CreateConnection();
        await connection.OpenAsync(cancellationToken);
        await OnOpenedAsync(connection, cancellationToken);
        return connection;
    }

    public virtual async Task<DbTransaction> BeginWriteTransactionAsync(DbConnection connection, CancellationToken cancellationToken) =>
        await connection.BeginTransactionAsync(cancellationToken);

    protected virtual Task OnOpenedAsync(DbConnection connection, CancellationToken cancellationToken) => Task.CompletedTask;

    public virtual ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

internal static class DbValue
{
    public static DateTimeOffset ToDateTimeOffset(object? value) => value switch
    {
        DateTimeOffset dto => dto.ToUniversalTime(),
        DateTime dt => new DateTimeOffset(dt.Kind == DateTimeKind.Unspecified ? DateTime.SpecifyKind(dt, DateTimeKind.Utc) : dt.ToUniversalTime()),
        string s => DateTimeOffset.Parse(s, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal).ToUniversalTime(),
        long ms => DateTimeOffset.FromUnixTimeMilliseconds(ms),
        null or DBNull => DateTimeOffset.MinValue,
        _ => throw new InvalidCastException($"Não foi possível converter {value.GetType().Name} em DateTimeOffset.")
    };

    public static DateTimeOffset? ToNullableDateTimeOffset(object? value) =>
        value is null or DBNull ? null : ToDateTimeOffset(value);

    public static byte[] ToArray(ReadOnlyMemory<byte> memory) =>
        System.Runtime.InteropServices.MemoryMarshal.TryGetArray(memory, out var segment)
        && segment.Offset == 0 && segment.Array is not null && segment.Count == segment.Array.Length
            ? segment.Array
            : memory.ToArray();
}
