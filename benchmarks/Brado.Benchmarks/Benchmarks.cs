using System.Threading.Channels;
using BenchmarkDotNet.Attributes;
using Brado.Broker;
using Brado.Broker.Metrics;
using Brado.EventStore;
using Brado.EventStore.Dialects;
using Brado.Protocol;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace Brado.Benchmarks;

[MemoryDiagnoser]
public class FrameCodecBenchmarks
{
    private Frame _frame = null!;
    private byte[] _buffer = null!;
    private byte[] _encoded = null!;

    [Params(64, 1024, 16 * 1024)]
    public int PayloadSize { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var payload = new byte[PayloadSize];
        Random.Shared.NextBytes(payload);
        _frame = new Frame(FrameType.Publish, 42, "telemetria.sensores", payload);
        _buffer = new byte[FrameCodec.GetEncodedSize(_frame)];
        _encoded = FrameCodec.Encode(_frame);
    }

    [Benchmark]
    public int EncodeIntoSpan() => FrameCodec.Encode(_frame, _buffer);

    [Benchmark]
    public FrameHeader ReadHeader() => FrameCodec.ReadHeader(_encoded);

    [Benchmark]
    public Frame Decode() => FrameCodec.Decode(_encoded);
}

[MemoryDiagnoser]
public class BrokerBenchmarks
{
    private MessageBroker _broker = null!;
    private byte[] _payload = null!;

    [Params(1_000, 10_000)]
    public int Messages { get; set; }

    [IterationSetup]
    public void Setup()
    {
        var options = new BrokerOptions { PersistMessages = false, RetainedMessagesPerTopic = 50_000 };
        _broker = new MessageBroker(new DefaultTopicFactory(options), new NullMessageJournal(), new BrokerMetrics(),
            Options.Create(options), NullLogger<MessageBroker>.Instance);
        _payload = new byte[256];
    }

    [Benchmark]
    public async Task PublishWithoutConsumers()
    {
        for (var i = 0; i < Messages; i++)
            await _broker.PublishOwnedAsync("bench", _payload);
    }

    [Benchmark]
    public async Task PublishAndConsumeLocally()
    {
        var done = new TaskCompletionSource();
        var received = 0;
        await using var subscription = _broker.SubscribeLocal("bench-consume", "g", (_, _) =>
        {
            if (Interlocked.Increment(ref received) == Messages) done.TrySetResult();
            return ValueTask.CompletedTask;
        }, prefetch: 512);

        for (var i = 0; i < Messages; i++)
            await _broker.PublishOwnedAsync("bench-consume", _payload);
        await done.Task;
    }

    [IterationCleanup]
    public void Cleanup() => _broker.DisposeAsync().AsTask().GetAwaiter().GetResult();
}

[MemoryDiagnoser]
public class ChannelBenchmarks
{
    [Params(100_000)]
    public int Items { get; set; }

    [Benchmark(Baseline = true)]
    public async Task BoundedChannelSingleReader()
    {
        var channel = Channel.CreateBounded<int>(new BoundedChannelOptions(8192) { SingleReader = true, SingleWriter = true });
        var reader = Task.Run(async () =>
        {
            var count = 0;
            await foreach (var _ in channel.Reader.ReadAllAsync()) count++;
            return count;
        });
        for (var i = 0; i < Items; i++) await channel.Writer.WriteAsync(i);
        channel.Writer.Complete();
        await reader;
    }

    [Benchmark]
    public async Task UnboundedChannelSingleReader()
    {
        var channel = Channel.CreateUnbounded<int>(new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });
        var reader = Task.Run(async () =>
        {
            var count = 0;
            await foreach (var _ in channel.Reader.ReadAllAsync()) count++;
            return count;
        });
        for (var i = 0; i < Items; i++) channel.Writer.TryWrite(i);
        channel.Writer.Complete();
        await reader;
    }
}

[MemoryDiagnoser]
public class EventStoreBenchmarks
{
    private string _path = null!;
    private SqliteDialect _dialect = null!;
    private SqlEventStore _store = null!;
    private long _version;
    private string _stream = null!;

    [Params(1, 10)]
    public int EventsPerAppend { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _path = Path.Combine(Path.GetTempPath(), $"brado-bench-{Guid.NewGuid():N}.db");
        _dialect = new SqliteDialect($"Data Source={_path}");
        EventStoreSchema.EnsureCreatedAsync(_dialect).GetAwaiter().GetResult();
        _store = new SqlEventStore(_dialect);
        _stream = $"bench-{Guid.NewGuid():N}";
    }

    [Benchmark]
    public async Task AppendWithOutbox()
    {
        var events = Enumerable.Range(0, EventsPerAppend)
            .Select(_ => new EventData(Guid.NewGuid(), "BenchEvent", "{\"valor\":42}", "{}", DateTimeOffset.UtcNow))
            .ToArray();
        var outbox = events.Select(e => new OutboxMessage(e.EventId, "bench.events", "{}", "{}")).ToArray();
        _version = await _store.AppendAsync(_stream, "bench", _version, events, outbox);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _dialect.DisposeAsync().AsTask().GetAwaiter().GetResult();
        SqliteConnection.ClearAllPools();
        foreach (var file in new[] { _path, _path + "-wal", _path + "-shm" })
            if (File.Exists(file)) File.Delete(file);
    }
}
