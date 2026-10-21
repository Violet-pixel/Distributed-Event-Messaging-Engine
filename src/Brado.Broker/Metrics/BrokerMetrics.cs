namespace Brado.Broker.Metrics;

public sealed record ThroughputPoint(DateTimeOffset At, double InPerSecond, double OutPerSecond);

public sealed record MetricsSnapshot(
    DateTimeOffset StartedAt,
    long MessagesIn,
    long MessagesOut,
    long BytesIn,
    long BytesOut,
    long Acked,
    long Nacked,
    long Redelivered,
    long DeadLettered,
    double InPerSecond,
    double OutPerSecond,
    IReadOnlyList<ThroughputPoint> Series);

public sealed class BrokerMetrics
{
    public const int SeriesLength = 120;

    private long _messagesIn, _messagesOut, _bytesIn, _bytesOut;
    private long _acked, _nacked, _redelivered, _deadLettered;
    private long _lastIn, _lastOut;
    private DateTimeOffset _lastSample = DateTimeOffset.UtcNow;
    private readonly object _gate = new();
    private readonly Queue<ThroughputPoint> _series = new();
    private ThroughputPoint _current = new(DateTimeOffset.UtcNow, 0, 0);

    public DateTimeOffset StartedAt { get; } = DateTimeOffset.UtcNow;

    public void Published(int bytes)
    {
        Interlocked.Increment(ref _messagesIn);
        Interlocked.Add(ref _bytesIn, bytes);
    }

    public void Delivered(int bytes)
    {
        Interlocked.Increment(ref _messagesOut);
        Interlocked.Add(ref _bytesOut, bytes);
    }

    public void Acked() => Interlocked.Increment(ref _acked);
    public void Nacked() => Interlocked.Increment(ref _nacked);
    public void Redelivered() => Interlocked.Increment(ref _redelivered);
    public void DeadLettered() => Interlocked.Increment(ref _deadLettered);

    public void Sample()
    {
        lock (_gate)
        {
            var now = DateTimeOffset.UtcNow;
            var seconds = Math.Max(0.001, (now - _lastSample).TotalSeconds);
            var totalIn = Interlocked.Read(ref _messagesIn);
            var totalOut = Interlocked.Read(ref _messagesOut);

            _current = new ThroughputPoint(now, Math.Round((totalIn - _lastIn) / seconds, 1), Math.Round((totalOut - _lastOut) / seconds, 1));
            _series.Enqueue(_current);
            while (_series.Count > SeriesLength) _series.Dequeue();

            _lastIn = totalIn;
            _lastOut = totalOut;
            _lastSample = now;
        }
    }

    public MetricsSnapshot Snapshot()
    {
        lock (_gate)
        {
            return new MetricsSnapshot(
                StartedAt,
                Interlocked.Read(ref _messagesIn),
                Interlocked.Read(ref _messagesOut),
                Interlocked.Read(ref _bytesIn),
                Interlocked.Read(ref _bytesOut),
                Interlocked.Read(ref _acked),
                Interlocked.Read(ref _nacked),
                Interlocked.Read(ref _redelivered),
                Interlocked.Read(ref _deadLettered),
                _current.InPerSecond,
                _current.OutPerSecond,
                _series.ToArray());
        }
    }
}
