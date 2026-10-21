using System.Diagnostics.CodeAnalysis;

namespace Brado.Broker;

public interface ITopicObserver
{
    void OnAppended(Topic topic, long offset);
}

public interface ITopicFactory
{
    Topic Create(string name);
}

public sealed class DefaultTopicFactory(BrokerOptions options) : ITopicFactory
{
    public Topic Create(string name) => new(name, options.RetainedMessagesPerTopic);
}

public sealed class Topic
{
    private readonly object _gate = new();
    private readonly List<StoredMessage> _log = [];
    private readonly int _retention;
    private readonly object _groupsGate = new();
    private readonly Dictionary<string, ConsumerGroup> _groups = new(StringComparer.Ordinal);
    private ITopicObserver[] _observers = [];
    private long _firstOffset;
    private long _nextOffset;

    public Topic(string name, int retention)
    {
        Name = name;
        _retention = Math.Max(16, retention);
    }

    public string Name { get; }

    public long NextOffset => Interlocked.Read(ref _nextOffset);

    public long FirstOffset { get { lock (_gate) return _firstOffset; } }

    public int Retained { get { lock (_gate) return _log.Count; } }

    public StoredMessage Append(ReadOnlyMemory<byte> payload)
    {
        StoredMessage message;
        lock (_gate)
        {
            message = new StoredMessage(_nextOffset, payload, DateTimeOffset.UtcNow);
            _log.Add(message);
            Interlocked.Increment(ref _nextOffset);

            var excess = _log.Count - _retention;
            if (excess > _retention / 10)
            {
                _log.RemoveRange(0, excess);
                _firstOffset += excess;
            }
        }

        foreach (var observer in Volatile.Read(ref _observers))
            observer.OnAppended(this, message.Offset);

        return message;
    }

    public bool TryGet(long offset, out StoredMessage message)
    {
        lock (_gate)
        {
            var index = offset - _firstOffset;
            if (index >= 0 && index < _log.Count)
            {
                message = _log[(int)index];
                return true;
            }
        }
        message = null!;
        return false;
    }

    public IReadOnlyList<StoredMessage> Read(long fromOffset, int max)
    {
        lock (_gate)
        {
            var start = (int)Math.Max(0, fromOffset - _firstOffset);
            if (start >= _log.Count) return [];
            return _log.GetRange(start, Math.Min(max, _log.Count - start));
        }
    }

    public void Restore(IReadOnlyList<StoredMessage> messages)
    {
        if (messages.Count == 0) return;
        lock (_gate)
        {
            _log.Clear();
            _log.AddRange(messages.OrderBy(m => m.Offset));
            _firstOffset = _log[0].Offset;
            Interlocked.Exchange(ref _nextOffset, _log[^1].Offset + 1);
        }
    }

    public ConsumerGroup GetOrAddGroup(string name, Func<string, ConsumerGroup> factory)
    {
        lock (_groupsGate)
        {
            if (_groups.TryGetValue(name, out var existing)) return existing;
            var group = factory(name);
            _groups[name] = group;
            Subscribe(group);
            return group;
        }
    }

    public bool TryGetGroup(string name, [MaybeNullWhen(false)] out ConsumerGroup group)
    {
        lock (_groupsGate) return _groups.TryGetValue(name, out group);
    }

    public IReadOnlyList<ConsumerGroup> Groups
    {
        get { lock (_groupsGate) return _groups.Values.ToArray(); }
    }

    public void Subscribe(ITopicObserver observer)
    {
        lock (_groupsGate)
            Volatile.Write(ref _observers, [.. _observers, observer]);
    }
}
