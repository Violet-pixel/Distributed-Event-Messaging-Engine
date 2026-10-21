using Brado.Cqrs.ReadModel;
using Brado.Domain;
using Brado.Domain.Events;
using Brado.EventStore;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace Brado.Cqrs.Projections;

public interface IProjection
{
    string Name { get; }
    Task ApplyAsync(RecordedEvent recorded, IDomainEvent @event, ReadModelDbContext db, CancellationToken cancellationToken);
}

internal static class Money
{
    public static long ToCents(decimal value) => (long)Math.Round(value * 100m, MidpointRounding.AwayFromZero);
    public static decimal FromCents(long cents) => cents / 100m;
}

public sealed class OrderSummaryProjection : IProjection
{
    public string Name => "order-summary";

    public async Task ApplyAsync(RecordedEvent recorded, IDomainEvent @event, ReadModelDbContext db, CancellationToken cancellationToken)
    {
        if (@event is OrderCreated created)
        {
            if (await db.Orders.FindAsync([created.OrderId], cancellationToken) is not null) return;
            db.Orders.Add(new OrderSummaryView
            {
                OrderId = created.OrderId,
                CustomerRef = created.CustomerRef,
                Currency = created.Currency,
                Status = nameof(OrderStatus.Draft),
                Version = recorded.StreamVersion,
                CreatedAt = created.CreatedAt.UtcDateTime,
                UpdatedAt = recorded.OccurredAt.UtcDateTime
            });
            return;
        }

        if (@event is not IOrderEvent orderEvent) return;
        var order = await db.Orders.FindAsync([orderEvent.OrderId], cancellationToken);
        if (order is null || order.Version >= recorded.StreamVersion) return;

        switch (@event)
        {
            case OrderItemAdded e:
                order.ItemCount += e.Quantity;
                order.TotalCents += Money.ToCents(e.UnitPrice) * e.Quantity;
                db.OrderLines.Add(new OrderLineView { OrderId = e.OrderId, Sku = e.Sku, Quantity = e.Quantity, UnitPriceCents = Money.ToCents(e.UnitPrice) });
                break;
            case OrderSubmitted:
                order.Status = nameof(OrderStatus.Submitted);
                break;
            case PaymentProcessed e:
                order.Status = nameof(OrderStatus.Paid);
                order.PaymentRef = e.PaymentRef;
                break;
            case PaymentFailed e:
                order.FailureReason = e.Reason;
                break;
            case StockReserved:
                order.Status = nameof(OrderStatus.StockReserved);
                break;
            case StockReservationFailed e:
                order.FailureReason = e.Reason;
                break;
            case PaymentRefunded:
                order.FailureReason = (order.FailureReason ?? "") + " · estornado";
                break;
            case OrderShipped e:
                order.Status = nameof(OrderStatus.Shipped);
                order.TrackingCode = e.TrackingCode;
                break;
            case OrderCancelled e:
                order.Status = nameof(OrderStatus.Cancelled);
                order.FailureReason ??= e.Reason;
                break;
        }

        order.Version = recorded.StreamVersion;
        order.UpdatedAt = recorded.OccurredAt.UtcDateTime;
    }
}

public sealed class DailySalesProjection : IProjection
{
    public string Name => "daily-sales";

    public async Task ApplyAsync(RecordedEvent recorded, IDomainEvent @event, ReadModelDbContext db, CancellationToken cancellationToken)
    {
        switch (@event)
        {
            case PaymentProcessed e:
            {
                var day = await GetDayAsync(db, e.ProcessedAt, cancellationToken);
                day.PaidOrders++;
                day.RevenueCents += Money.ToCents(e.Amount);
                break;
            }
            case PaymentRefunded e:
            {
                var day = await GetDayAsync(db, e.RefundedAt, cancellationToken);
                day.Refunds++;
                day.RefundedCents += Money.ToCents(e.Amount);
                break;
            }
        }
    }

    private static async Task<DailySalesView> GetDayAsync(ReadModelDbContext db, DateTimeOffset at, CancellationToken cancellationToken)
    {
        var key = DateOnly.FromDateTime(at.UtcDateTime);
        var day = await db.DailySales.FindAsync([key], cancellationToken);
        if (day is not null) return day;
        day = new DailySalesView { Day = key };
        db.DailySales.Add(day);
        return day;
    }
}

public sealed class ProjectionEngine(
    IEnumerable<IProjection> projections,
    EventTypeRegistry registry,
    IEventStore store,
    ILogger<ProjectionEngine> logger)
{
    public const string CheckpointName = "read-model";

    private readonly IProjection[] _projections = projections.ToArray();
    private readonly SemaphoreSlim _gate = new(1, 1);
    private long _position;

    public long Position => Interlocked.Read(ref _position);

    public async Task<int> ProcessBatchAsync(ReadModelDbContext db, int batchSize, CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken);
        try
        {
            var checkpoint = await db.Checkpoints.FindAsync([CheckpointName], cancellationToken);
            var from = checkpoint?.Position ?? 0;
            var events = await store.ReadAllAsync(from, batchSize, cancellationToken);
            if (events.Count == 0)
            {
                Interlocked.Exchange(ref _position, from);
                return 0;
            }

            if (checkpoint is null)
            {
                checkpoint = new ProjectionCheckpoint { Projection = CheckpointName };
                db.Checkpoints.Add(checkpoint);
            }

            await using var transaction = await db.Database.BeginTransactionAsync(cancellationToken);
            foreach (var recorded in events)
            {
                if (registry.TryDeserialize(recorded.EventType, recorded.Payload, out var @event) && @event is not null)
                {
                    foreach (var projection in _projections)
                        await projection.ApplyAsync(recorded, @event, db, cancellationToken);
                }
                else
                {
                    logger.LogWarning("Evento {Type} na posição {Position} ignorado: tipo desconhecido", recorded.EventType, recorded.GlobalPosition);
                }

                checkpoint.Position = recorded.GlobalPosition;
            }

            checkpoint.UpdatedAt = DateTime.UtcNow;
            await db.SaveChangesAsync(cancellationToken);
            await transaction.CommitAsync(cancellationToken);

            Interlocked.Exchange(ref _position, checkpoint.Position);
            return events.Count;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task ResetAsync(ReadModelDbContext db, CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var transaction = await db.Database.BeginTransactionAsync(cancellationToken);
            await db.OrderLines.ExecuteDeleteAsync(cancellationToken);
            await db.Orders.ExecuteDeleteAsync(cancellationToken);
            await db.DailySales.ExecuteDeleteAsync(cancellationToken);
            await db.Checkpoints.ExecuteDeleteAsync(cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            Interlocked.Exchange(ref _position, 0);
            logger.LogInformation("Modelo de leitura zerado; replay completo agendado");
        }
        finally
        {
            _gate.Release();
        }
    }
}
