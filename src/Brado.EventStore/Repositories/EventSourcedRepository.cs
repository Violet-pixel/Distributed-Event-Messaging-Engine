using System.Diagnostics;
using System.Text.Json;
using Brado.Domain;
using Brado.Domain.Contracts;
using Microsoft.Extensions.Options;

namespace Brado.EventStore.Repositories;

public interface IAggregateRepository<T> where T : AggregateRoot, new()
{
    Task<T?> LoadAsync(Guid id, CancellationToken cancellationToken = default);

    Task<long> SaveAsync(T aggregate, CancellationToken cancellationToken = default);
}

public sealed class EventSourcedRepository<T>(
    IEventStore store,
    EventTypeRegistry registry,
    EventAppendedSignal signal,
    IOptions<EventStoreOptions> options) : IAggregateRepository<T> where T : AggregateRoot, new()
{
    public async Task<T?> LoadAsync(Guid id, CancellationToken cancellationToken = default)
    {
        var aggregate = new T();
        var streamId = AggregateRoot.StreamIdFor(aggregate.StreamType, id);
        long afterVersion = 0;

        if (aggregate is ISnapshotable snapshotable && await store.LoadSnapshotAsync(streamId, cancellationToken) is { } snapshot)
        {
            var state = JsonSerializer.Deserialize(snapshot.Payload, snapshotable.SnapshotType, Json.Options);
            if (state is not null)
            {
                snapshotable.RestoreSnapshot(state, snapshot.Version);
                afterVersion = snapshot.Version;
            }
        }

        var events = await store.ReadStreamAsync(streamId, afterVersion, cancellationToken);
        if (afterVersion == 0 && events.Count == 0) return null;

        aggregate.LoadFromHistory(events.Select(e => registry.Deserialize(e.EventType, e.Payload)));
        return aggregate;
    }

    public async Task<long> SaveAsync(T aggregate, CancellationToken cancellationToken = default)
    {
        var pending = aggregate.UncommittedEvents;
        if (pending.Count == 0) return aggregate.Version;

        var now = DateTimeOffset.UtcNow;
        var correlationId = Activity.Current?.TraceId.ToString() ?? Guid.NewGuid().ToString("N");
        var events = new List<EventData>(pending.Count);
        var outbox = new List<OutboxMessage>(pending.Count);
        var version = aggregate.Version;

        foreach (var @event in pending)
        {
            version++;
            var eventId = Guid.CreateVersion7(now);
            var eventType = registry.GetName(@event.GetType());
            var metadata = JsonSerializer.Serialize(new { aggregateType = aggregate.StreamType, schemaVersion = 1, correlationId }, Json.Options);

            events.Add(new EventData(eventId, eventType, registry.Serialize(@event), metadata, now));

            var envelope = new EventEnvelope(eventId, eventType, aggregate.StreamId, version, now,
                JsonSerializer.SerializeToElement(@event, @event.GetType(), Json.Options));
            var headers = JsonSerializer.Serialize(new { contentType = "application/json", eventType, correlationId }, Json.Options);
            outbox.Add(new OutboxMessage(eventId, aggregate.Topic, JsonSerializer.Serialize(envelope, Json.Options), headers));
        }

        var previousVersion = aggregate.Version;
        var newVersion = await store.AppendAsync(aggregate.StreamId, aggregate.StreamType, previousVersion, events, outbox, cancellationToken);
        aggregate.MarkCommitted();
        signal.Notify();

        var every = options.Value.SnapshotEvery;
        if (aggregate is ISnapshotable snapshotable && every > 0 && newVersion / every > previousVersion / every)
        {
            var payload = JsonSerializer.Serialize(snapshotable.TakeSnapshot(), snapshotable.SnapshotType, Json.Options);
            await store.SaveSnapshotAsync(new Snapshot(aggregate.StreamId, newVersion, payload, DateTimeOffset.UtcNow), cancellationToken);
        }

        return newVersion;
    }
}
