namespace Brado.Domain;

public interface IDomainEvent
{
}

public abstract class AggregateRoot
{
    private readonly List<IDomainEvent> _uncommitted = [];

    public Guid Id { get; protected set; }

    public long Version { get; private set; }

    public abstract string StreamType { get; }

    public virtual string Topic => $"{StreamType}s.events";

    public string StreamId => StreamIdFor(StreamType, Id);

    public IReadOnlyList<IDomainEvent> UncommittedEvents => _uncommitted;

    public static string StreamIdFor(string streamType, Guid id) => $"{streamType}-{id:N}";

    protected void Raise(IDomainEvent @event)
    {
        Apply(@event);
        _uncommitted.Add(@event);
    }

    protected abstract void Apply(IDomainEvent @event);

    public void LoadFromHistory(IEnumerable<IDomainEvent> history)
    {
        foreach (var @event in history)
        {
            Apply(@event);
            Version++;
        }
    }

    public void MarkCommitted()
    {
        Version += _uncommitted.Count;
        _uncommitted.Clear();
    }

    protected void RestoreVersion(long version) => Version = version;
}

public interface ISnapshotable
{
    Type SnapshotType { get; }
    object TakeSnapshot();
    void RestoreSnapshot(object snapshot, long version);
}

public sealed class DomainException(string message) : Exception(message);
