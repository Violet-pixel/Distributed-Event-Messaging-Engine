using Brado.Domain;
using Brado.EventStore;
using Brado.EventStore.Dialects;
using Brado.EventStore.Outbox;
using Brado.EventStore.Repositories;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Options;

namespace Brado.UnitTests;

public sealed class SqliteEventStoreTests : IAsyncLifetime
{
    private readonly string _path = TestBroker.TempFile("events");
    private SqliteDialect _dialect = null!;
    private SqlEventStore _store = null!;

    public async ValueTask InitializeAsync()
    {
        _dialect = new SqliteDialect($"Data Source={_path}");
        await EventStoreSchema.EnsureCreatedAsync(_dialect);
        _store = new SqlEventStore(_dialect);
    }

    public async ValueTask DisposeAsync()
    {
        await _dialect.DisposeAsync();
        SqliteConnection.ClearAllPools();
        foreach (var file in new[] { _path, _path + "-wal", _path + "-shm" })
            if (File.Exists(file)) File.Delete(file);
    }

    private static EventData Event(string type = "Teste", string payload = "{\"valor\":1}") =>
        new(Guid.NewGuid(), type, payload, "{}", DateTimeOffset.UtcNow);

    [Fact]
    public async Task Append_and_read_back_in_order()
    {
        var version = await _store.AppendAsync("conta-1", "conta", ExpectedVersion.NoStream, [Event("Aberta"), Event("Depositado")]);
        version = await _store.AppendAsync("conta-1", "conta", version, [Event("Sacado")]);

        var events = await _store.ReadStreamAsync("conta-1");

        Assert.Equal(3, version);
        Assert.Equal(new[] { "Aberta", "Depositado", "Sacado" }, events.Select(e => e.EventType));
        Assert.Equal(new long[] { 1, 2, 3 }, events.Select(e => e.StreamVersion));
        Assert.True(events.Zip(events.Skip(1)).All(p => p.First.GlobalPosition < p.Second.GlobalPosition));
    }

    [Fact]
    public async Task Wrong_expected_version_raises_concurrency_exception()
    {
        await _store.AppendAsync("s", "t", ExpectedVersion.NoStream, [Event()]);
        var error = await Assert.ThrowsAsync<ConcurrencyException>(() => _store.AppendAsync("s", "t", ExpectedVersion.NoStream, [Event()]));
        Assert.Equal(1, error.Actual);
    }

    [Fact]
    public async Task Events_table_is_append_only()
    {
        await _store.AppendAsync("s", "t", ExpectedVersion.NoStream, [Event()]);
        await using var connection = await _dialect.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = "UPDATE events SET event_type = 'adulterado'";
        await Assert.ThrowsAsync<SqliteException>(() => command.ExecuteNonQueryAsync());
    }

    [Fact]
    public async Task Outbox_is_written_atomically_and_can_be_claimed_once()
    {
        var outbox = new SqlOutboxStore(_dialect);
        var message = new OutboxMessage(Guid.NewGuid(), "orders.events", "{\"ok\":true}", "{}");
        await _store.AppendAsync("s", "t", ExpectedVersion.NoStream, [Event()], [message]);

        var first = await outbox.ClaimAsync("w1", 10, TimeSpan.FromMinutes(1));
        var second = await outbox.ClaimAsync("w2", 10, TimeSpan.FromMinutes(1));

        Assert.Single(first);
        Assert.Equal(message.MessageId, first[0].MessageId);
        Assert.Empty(second);

        await outbox.MarkDispatchedAsync([first[0].Id]);
        var stats = await outbox.GetStatsAsync();
        Assert.Equal(0, stats.Pending);
        Assert.Equal(1, stats.Dispatched);
    }

    [Fact]
    public async Task Failed_outbox_message_is_rescheduled_not_lost()
    {
        var outbox = new SqlOutboxStore(_dialect);
        await _store.AppendAsync("s", "t", ExpectedVersion.NoStream, [Event()], [new OutboxMessage(Guid.NewGuid(), "x", "{}", "{}")]);
        var claimed = await outbox.ClaimAsync("w", 10, TimeSpan.FromMinutes(1));

        await outbox.MarkFailedAsync([new OutboxFailure(claimed[0].Id, "broker fora", DateTimeOffset.UtcNow.AddMilliseconds(-1))]);
        var retried = await outbox.ClaimAsync("w", 10, TimeSpan.FromMinutes(1));

        Assert.Single(retried);
        Assert.Equal(1, retried[0].Attempts);
    }

    [Fact]
    public async Task Repository_saves_with_outbox_and_rehydrates_through_snapshot()
    {
        var registry = new EventTypeRegistry().RegisterAssembly(typeof(Order).Assembly);
        var repository = new EventSourcedRepository<Order>(_store, registry, new EventAppendedSignal(),
            Options.Create(new EventStoreOptions { SnapshotEvery = 3 }));

        var order = Order.Create(Guid.NewGuid(), "CLI-77");
        order.AddItem("SKU-1", 1, 10m);
        order.AddItem("SKU-2", 2, 5m);
        order.Submit();
        await repository.SaveAsync(order);

        var snapshot = await _store.LoadSnapshotAsync(order.StreamId);
        var loaded = await repository.LoadAsync(order.Id);
        var outboxStats = await new SqlOutboxStore(_dialect).GetStatsAsync();

        Assert.NotNull(snapshot);
        Assert.Equal(4, snapshot!.Version);
        Assert.NotNull(loaded);
        Assert.Equal(OrderStatus.Submitted, loaded!.Status);
        Assert.Equal(20m, loaded.Total);
        Assert.Equal(4, loaded.Version);
        Assert.Equal(4, outboxStats.Pending);
    }
}
