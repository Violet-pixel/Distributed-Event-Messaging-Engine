using System.Collections.Concurrent;
using Brado.Broker;
using Brado.EventStore;
using Brado.EventStore.Dialects;
using Brado.EventStore.Journal;
using Brado.EventStore.Outbox;
using Brado.Workers;
using Brado.Workers.Outbox;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;
using Testcontainers.PostgreSql;

namespace Brado.IntegrationTests;

public sealed class PostgresFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder()
        .WithImage("postgres:17-alpine")
        .WithDatabase("brado")
        .WithUsername("brado")
        .WithPassword("brado")
        .Build();

    public PostgresDialect Dialect { get; private set; } = null!;

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();
        Dialect = new PostgresDialect(_container.GetConnectionString());
        await EventStoreSchema.EnsureCreatedAsync(Dialect);
        await EventStoreSchema.EnsureCreatedAsync(Dialect);
    }

    public async ValueTask DisposeAsync()
    {
        await Dialect.DisposeAsync();
        await _container.DisposeAsync();
    }
}

public sealed class PostgresTests(PostgresFixture fixture) : IClassFixture<PostgresFixture>
{
    private static EventData Event(string type) => new(Guid.NewGuid(), type, "{\"n\":1}", "{}", DateTimeOffset.UtcNow);

    private static string Stream() => $"teste-{Guid.NewGuid():N}";

    [Fact]
    public async Task Append_read_and_global_order()
    {
        var store = new SqlEventStore(fixture.Dialect);
        var stream = Stream();

        await store.AppendAsync(stream, "teste", ExpectedVersion.NoStream, [Event("A"), Event("B")]);
        await store.AppendAsync(stream, "teste", 2, [Event("C")]);
        var events = await store.ReadStreamAsync(stream);

        Assert.Equal(new[] { "A", "B", "C" }, events.Select(e => e.EventType));
        Assert.True(events[0].GlobalPosition < events[2].GlobalPosition);
    }

    [Fact]
    public async Task Concurrent_writers_on_the_same_stream_only_one_wins()
    {
        var store = new SqlEventStore(fixture.Dialect);
        var stream = Stream();
        await store.AppendAsync(stream, "teste", ExpectedVersion.NoStream, [Event("Inicial")]);

        var attempts = Enumerable.Range(0, 8).Select(async i =>
        {
            try { await store.AppendAsync(stream, "teste", 1, [Event($"Escritor{i}")]); return true; }
            catch (ConcurrencyException) { return false; }
        });
        var results = await Task.WhenAll(attempts);

        Assert.Equal(1, results.Count(r => r));
        Assert.Equal(2, (await store.ReadStreamAsync(stream)).Count);
    }

    [Fact]
    public async Task Update_on_events_is_blocked_by_trigger()
    {
        var store = new SqlEventStore(fixture.Dialect);
        var stream = Stream();
        await store.AppendAsync(stream, "teste", ExpectedVersion.NoStream, [Event("A")]);

        await using var connection = await fixture.Dialect.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = $"DELETE FROM events WHERE stream_id = '{stream}'";
        await Assert.ThrowsAsync<PostgresException>(() => command.ExecuteNonQueryAsync());
    }

    [Fact]
    public async Task Two_workers_never_claim_the_same_outbox_message()
    {
        var store = new SqlEventStore(fixture.Dialect);
        var outbox = new SqlOutboxStore(fixture.Dialect);
        var topic = $"t-{Guid.NewGuid():N}";
        var messages = Enumerable.Range(0, 60).Select(_ => new OutboxMessage(Guid.NewGuid(), topic, "{}", "{}")).ToArray();
        await store.AppendAsync(Stream(), "teste", ExpectedVersion.NoStream, [Event("Lote")], messages);

        var claimed = new ConcurrentBag<Guid>();
        await Task.WhenAll(Enumerable.Range(0, 4).Select(async w =>
        {
            while (true)
            {
                var batch = await outbox.ClaimAsync($"w{w}", 7, TimeSpan.FromMinutes(5));
                var mine = batch.Where(m => m.Topic == topic).ToArray();
                if (batch.Count == 0) break;
                foreach (var m in mine) claimed.Add(m.MessageId);
                await outbox.MarkDispatchedAsync(batch.Select(m => m.Id).ToArray());
            }
        }));

        Assert.Equal(60, claimed.Count);
        Assert.Equal(60, claimed.Distinct().Count());
    }

    [Fact]
    public async Task Dispatcher_retries_until_broker_accepts_and_loses_nothing()
    {
        var store = new SqlEventStore(fixture.Dialect);
        var outboxStore = new SqlOutboxStore(fixture.Dialect);
        var topic = $"retry-{Guid.NewGuid():N}";
        await store.AppendAsync(Stream(), "teste", ExpectedVersion.NoStream, [Event("X")],
            Enumerable.Range(0, 10).Select(_ => new OutboxMessage(Guid.NewGuid(), topic, "{}", "{}")).ToArray());

        var publisher = new FlakyPublisher(failuresBeforeSuccess: 3);
        var options = Options.Create(new OutboxOptions { BatchSize = 50, BaseRetryDelay = TimeSpan.FromMilliseconds(1), MaxRetryDelay = TimeSpan.FromMilliseconds(5) });
        var dispatcher = new OutboxDispatcherService(outboxStore, publisher, new EventAppendedSignal(),
            new ExponentialBackoffRetryPolicy(options), new OutboxMetrics(), options, NullLogger<OutboxDispatcherService>.Instance);

        for (var i = 0; i < 50 && publisher.Delivered(topic) < 10; i++)
        {
            await dispatcher.DispatchBatchAsync(CancellationToken.None);
            await Task.Delay(10);
        }

        Assert.Equal(10, publisher.Delivered(topic));
    }

    [Fact]
    public async Task Journal_persists_messages_and_offsets()
    {
        var options = Options.Create(new BrokerOptions());
        var topic = $"journal-{Guid.NewGuid():N}";
        await using (var journal = new SqlMessageJournal(fixture.Dialect, options, NullLogger<SqlMessageJournal>.Instance))
        {
            for (var i = 0; i < 25; i++)
                await journal.AppendAsync(topic, new StoredMessage(i, new byte[] { (byte)i }, DateTimeOffset.UtcNow));
            await journal.SaveOffsetsAsync([new GroupOffset(topic, "g", 17)]);
        }

        await using var reader = new SqlMessageJournal(fixture.Dialect, options, NullLogger<SqlMessageJournal>.Instance);
        var tail = await reader.ReadTailAsync(topic, 10);
        var offsets = await reader.LoadOffsetsAsync();

        Assert.Equal(Enumerable.Range(15, 10).Select(i => (long)i), tail.Select(m => m.Offset));
        Assert.Contains(offsets, o => o.Topic == topic && o.Group == "g" && o.CommittedOffset == 17);
    }

    private sealed class FlakyPublisher(int failuresBeforeSuccess) : IMessagePublisher
    {
        private readonly ConcurrentDictionary<string, int> _delivered = new();
        private int _calls;

        public int Delivered(string topic) => _delivered.GetValueOrDefault(topic);

        public Task PublishAsync(string topic, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken = default)
        {
            if (Interlocked.Increment(ref _calls) <= failuresBeforeSuccess)
                throw new IOException("broker indisponível");
            _delivered.AddOrUpdate(topic, 1, (_, n) => n + 1);
            return Task.CompletedTask;
        }
    }
}
