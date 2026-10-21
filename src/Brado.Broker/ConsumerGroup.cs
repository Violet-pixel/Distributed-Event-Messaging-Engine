using System.Threading.Channels;
using Brado.Broker.Metrics;
using Microsoft.Extensions.Logging;

namespace Brado.Broker;

public sealed record ConsumerGroupSnapshot(
    string Name,
    int Members,
    long Committed,
    long Lag,
    int InFlight,
    int PendingRedelivery);

public sealed class ConsumerGroup : ITopicObserver, IAsyncDisposable
{
    private sealed class Member(IDeliveryTarget target)
    {
        public IDeliveryTarget Target { get; } = target;
        public int Credits { get; set; } = Math.Max(1, target.Prefetch);
    }

    private sealed record InFlight(long Offset, Member Member, DateTimeOffset Deadline);

    private readonly Topic _topic;
    private readonly BrokerOptions _options;
    private readonly BrokerMetrics _metrics;
    private readonly Func<string, StoredMessage, ValueTask> _deadLetter;
    private readonly ILogger _logger;

    private readonly object _gate = new();
    private readonly List<Member> _members = [];
    private readonly SortedSet<long> _redeliver = [];
    private readonly Dictionary<long, InFlight> _inFlight = new();
    private readonly Dictionary<long, int> _attempts = new();
    private int _roundRobin;
    private long _nextDispatch;

    private readonly Channel<bool> _signal = Channel.CreateBounded<bool>(new BoundedChannelOptions(1)
    {
        FullMode = BoundedChannelFullMode.DropWrite,
        SingleReader = true
    });
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _pump;

    public ConsumerGroup(
        Topic topic,
        string name,
        long startOffset,
        BrokerOptions options,
        BrokerMetrics metrics,
        Func<string, StoredMessage, ValueTask> deadLetter,
        ILogger logger)
    {
        _topic = topic;
        Name = name;
        _options = options;
        _metrics = metrics;
        _deadLetter = deadLetter;
        _logger = logger;
        _nextDispatch = startOffset < 0 ? topic.NextOffset : Math.Max(startOffset, topic.FirstOffset);
        _pump = Task.Run(() => PumpAsync(_cts.Token));
    }

    public string Name { get; }
    public string TopicName => _topic.Name;

    public long Committed
    {
        get
        {
            lock (_gate) return ComputeCommitted();
        }
    }

    public void AddMember(IDeliveryTarget target)
    {
        lock (_gate) _members.Add(new Member(target));
        Signal();
    }

    public void RemoveMember(long subscriptionId)
    {
        lock (_gate)
        {
            var member = _members.Find(m => m.Target.SubscriptionId == subscriptionId);
            if (member is null) return;
            _members.Remove(member);

            foreach (var flight in _inFlight.Values.Where(f => f.Member == member).ToArray())
            {
                _inFlight.Remove(flight.Offset);
                _redeliver.Add(flight.Offset);
            }
        }
        Signal();
    }

    public bool Ack(long subscriptionId, long offset)
    {
        lock (_gate)
        {
            if (!_inFlight.TryGetValue(offset, out var flight) || flight.Member.Target.SubscriptionId != subscriptionId)
                return false;

            _inFlight.Remove(offset);
            _attempts.Remove(offset);
            flight.Member.Credits++;
        }
        _metrics.Acked();
        Signal();
        return true;
    }

    public bool Nack(long subscriptionId, long offset, bool requeue)
    {
        bool deadLetter;
        lock (_gate)
        {
            if (!_inFlight.TryGetValue(offset, out var flight) || flight.Member.Target.SubscriptionId != subscriptionId)
                return false;

            _inFlight.Remove(offset);
            flight.Member.Credits++;
            deadLetter = !ScheduleRedelivery(offset, requeue);
        }

        _metrics.Nacked();
        if (deadLetter) SendToDeadLetter(offset);
        Signal();
        return true;
    }

    public int ExpireInFlight(DateTimeOffset now)
    {
        List<long>? dead = null;
        var expired = 0;
        lock (_gate)
        {
            foreach (var flight in _inFlight.Values.Where(f => f.Deadline <= now).ToArray())
            {
                expired++;
                _inFlight.Remove(flight.Offset);
                flight.Member.Credits++;
                if (!ScheduleRedelivery(flight.Offset, requeue: true))
                    (dead ??= []).Add(flight.Offset);
            }
        }

        if (dead is not null)
            foreach (var offset in dead) SendToDeadLetter(offset);
        if (expired > 0) Signal();
        return expired;
    }

    public void Nudge()
    {
        bool pending;
        lock (_gate) pending = _redeliver.Count > 0 || _nextDispatch < _topic.NextOffset;
        if (pending) Signal();
    }

    public ConsumerGroupSnapshot Snapshot()
    {
        lock (_gate)
        {
            var committed = ComputeCommitted();
            return new ConsumerGroupSnapshot(Name, _members.Count, committed,
                Math.Max(0, _topic.NextOffset - committed), _inFlight.Count, _redeliver.Count);
        }
    }

    public void OnAppended(Topic topic, long offset) => Signal();

    private void Signal() => _signal.Writer.TryWrite(true);

    private bool ScheduleRedelivery(long offset, bool requeue)
    {
        var attempts = _attempts.GetValueOrDefault(offset);
        if (requeue && attempts < _options.MaxDeliveryAttempts)
        {
            _redeliver.Add(offset);
            _metrics.Redelivered();
            return true;
        }

        _attempts.Remove(offset);
        return false;
    }

    private void SendToDeadLetter(long offset)
    {
        if (!_topic.TryGet(offset, out var message)) return;
        _metrics.DeadLettered();
        _ = _deadLetter(_topic.Name, message).AsTask().ContinueWith(
            t => _logger.LogError(t.Exception, "Falha ao enviar offset {Offset} de {Topic} para a DLQ", offset, _topic.Name),
            TaskContinuationOptions.OnlyOnFaulted);
    }

    private long ComputeCommitted()
    {
        var committed = _nextDispatch;
        if (_redeliver.Count > 0) committed = Math.Min(committed, _redeliver.Min);
        foreach (var offset in _inFlight.Keys) committed = Math.Min(committed, offset);
        return committed;
    }

    private async Task PumpAsync(CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var _ in _signal.Reader.ReadAllAsync(cancellationToken))
            {
                try
                {
                    Dispatch();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Erro na entrega do grupo {Group} em {Topic}", Name, _topic.Name);
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    private void Dispatch()
    {
        var batch = new List<(Member Member, StoredMessage Message, int Attempt)>();
        lock (_gate)
        {
            while (true)
            {
                var member = NextMemberWithCredit();
                if (member is null) break;

                long offset;
                if (_redeliver.Count > 0)
                {
                    offset = _redeliver.Min;
                    _redeliver.Remove(offset);
                }
                else
                {
                    var first = _topic.FirstOffset;
                    if (_nextDispatch < first) _nextDispatch = first;
                    if (_nextDispatch >= _topic.NextOffset) break;
                    offset = _nextDispatch++;
                }

                if (!_topic.TryGet(offset, out var message))
                {
                    _attempts.Remove(offset);
                    continue;
                }

                var attempt = _attempts.GetValueOrDefault(offset) + 1;
                _attempts[offset] = attempt;
                member.Credits--;
                _inFlight[offset] = new InFlight(offset, member, DateTimeOffset.UtcNow + _options.AckTimeout);
                batch.Add((member, message, attempt));
            }
        }

        foreach (var (member, message, attempt) in batch)
        {
            if (member.Target.TryDeliver(_topic.Name, Name, message, attempt))
            {
                _metrics.Delivered(message.Payload.Length);
                continue;
            }

            lock (_gate)
            {
                if (_inFlight.Remove(message.Offset))
                {
                    member.Credits++;
                    _attempts[message.Offset] = attempt - 1;
                    _redeliver.Add(message.Offset);
                }
            }
        }
    }

    private Member? NextMemberWithCredit()
    {
        for (var i = 0; i < _members.Count; i++)
        {
            var index = (_roundRobin + i) % _members.Count;
            var member = _members[index];
            if (member.Credits > 0)
            {
                _roundRobin = index + 1;
                return member;
            }
        }
        return null;
    }

    public async ValueTask DisposeAsync()
    {
        _signal.Writer.TryComplete();
        await _cts.CancelAsync();
        try { await _pump; } catch (OperationCanceledException) { }
        _cts.Dispose();
    }
}
