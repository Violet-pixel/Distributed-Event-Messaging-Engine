using Brado.Broker.Metrics;
using Brado.Cqrs;
using Brado.Cqrs.Projections;
using Brado.Cqrs.Queries;
using Brado.Cqrs.ReadModel;
using Brado.EventStore;
using Brado.EventStore.Dialects;
using Brado.EventStore.Outbox;
using Brado.EventStore.Sagas;
using Brado.Workers.Outbox;
using Microsoft.Extensions.Options;

namespace Brado.Server.Api;

public sealed record OutboxView(long Pending, long Dispatched, long Failing, DateTimeOffset? OldestPending, long DispatchedTotal, long Failures, string? LastError);

public sealed record RecentEventView(long Position, string StreamId, long Version, string EventType, DateTimeOffset OccurredAt);

public sealed record DashboardSnapshot(
    BrokerSnapshot Broker,
    EventStoreStats EventStore,
    IReadOnlyList<RecentEventView> RecentEvents,
    OutboxView Outbox,
    ReadModelOverview ReadModel,
    IReadOnlyDictionary<string, long> Sagas,
    long ProjectionPosition,
    string EventStoreProvider,
    string ReadModelProvider);

public sealed class DashboardService(
    BrokerMonitor broker,
    IEventStore store,
    IOutboxStore outbox,
    OutboxMetrics outboxMetrics,
    ISagaStore sagas,
    ProjectionEngine projections,
    SqlDialect dialect,
    IOptions<ReadModelOptions> readModel,
    IServiceScopeFactory scopes)
{
    private readonly SemaphoreSlim _gate = new(1, 1);
    private (DateTimeOffset At, EventStoreStats Stats, IReadOnlyList<RecentEventView> Recent, OutboxStats Outbox, ReadModelOverview ReadModel, IReadOnlyDictionary<string, long> Sagas)? _cache;

    public async Task<DashboardSnapshot> BuildAsync(CancellationToken cancellationToken)
    {
        var data = await GetDataAsync(cancellationToken);
        return new DashboardSnapshot(
            broker.Snapshot(),
            data.Stats,
            data.Recent,
            new OutboxView(data.Outbox.Pending, data.Outbox.Dispatched, data.Outbox.Failing, data.Outbox.OldestPending,
                outboxMetrics.Dispatched, outboxMetrics.Failed, outboxMetrics.LastError),
            data.ReadModel,
            data.Sagas,
            projections.Position,
            dialect.Name,
            readModel.Value.Provider);
    }

    private async Task<(DateTimeOffset At, EventStoreStats Stats, IReadOnlyList<RecentEventView> Recent, OutboxStats Outbox, ReadModelOverview ReadModel, IReadOnlyDictionary<string, long> Sagas)>
        GetDataAsync(CancellationToken cancellationToken)
    {
        if (_cache is { } fresh && DateTimeOffset.UtcNow - fresh.At < TimeSpan.FromSeconds(2)) return fresh;

        await _gate.WaitAsync(cancellationToken);
        try
        {
            if (_cache is { } again && DateTimeOffset.UtcNow - again.At < TimeSpan.FromSeconds(2)) return again;

            var stats = await store.GetStatsAsync(cancellationToken);
            var recent = await store.ReadAllAsync(Math.Max(0, stats.LastPosition - 14), 14, cancellationToken);
            var outboxStats = await outbox.GetStatsAsync(cancellationToken);
            var sagaStates = await sagas.CountByStateAsync(cancellationToken);

            await using var scope = scopes.CreateAsyncScope();
            var queries = scope.ServiceProvider.GetRequiredService<IQueryDispatcher>();
            var overview = await queries.QueryAsync(new GetReadModelOverview(), cancellationToken);

            var recentView = recent.OrderByDescending(e => e.GlobalPosition)
                .Select(e => new RecentEventView(e.GlobalPosition, e.StreamId, e.StreamVersion, e.EventType, e.OccurredAt))
                .ToArray();

            _cache = (DateTimeOffset.UtcNow, stats, recentView, outboxStats, overview, sagaStates);
            return _cache.Value;
        }
        finally
        {
            _gate.Release();
        }
    }
}
