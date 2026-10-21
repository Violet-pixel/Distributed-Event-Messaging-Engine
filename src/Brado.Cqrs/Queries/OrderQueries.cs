using Brado.Cqrs.Projections;
using Brado.Cqrs.ReadModel;
using Microsoft.EntityFrameworkCore;

namespace Brado.Cqrs.Queries;

public sealed record OrderLineDto(string Sku, int Quantity, decimal UnitPrice, decimal Subtotal);

public sealed record OrderSummaryDto(
    Guid OrderId,
    string CustomerRef,
    string Currency,
    string Status,
    int ItemCount,
    decimal Total,
    string? TrackingCode,
    string? FailureReason,
    long Version,
    DateTime CreatedAt,
    DateTime UpdatedAt);

public sealed record OrderDetailsDto(OrderSummaryDto Summary, string? PaymentRef, IReadOnlyList<OrderLineDto> Lines);

public sealed record PagedResult<T>(IReadOnlyList<T> Items, int Page, int PageSize, int Total);

public sealed record DailySalesDto(DateOnly Day, int PaidOrders, decimal Revenue, int Refunds, decimal Refunded);

public sealed record ReadModelOverview(int TotalOrders, IReadOnlyDictionary<string, int> ByStatus, decimal Revenue, long Checkpoint);

public sealed record GetOrder(Guid OrderId) : IQuery<OrderDetailsDto?>;

public sealed record ListOrders(string? Status = null, int Page = 1, int PageSize = 20) : IQuery<PagedResult<OrderSummaryDto>>;

public sealed record GetDailySales(int Days = 14) : IQuery<IReadOnlyList<DailySalesDto>>;

public sealed record GetReadModelOverview : IQuery<ReadModelOverview>;

public sealed class OrderQueryHandlers(ReadModelDbContext db) :
    IQueryHandler<GetOrder, OrderDetailsDto?>,
    IQueryHandler<ListOrders, PagedResult<OrderSummaryDto>>,
    IQueryHandler<GetDailySales, IReadOnlyList<DailySalesDto>>,
    IQueryHandler<GetReadModelOverview, ReadModelOverview>
{
    public async Task<OrderDetailsDto?> HandleAsync(GetOrder query, CancellationToken cancellationToken)
    {
        var order = await db.Orders.AsNoTracking().FirstOrDefaultAsync(o => o.OrderId == query.OrderId, cancellationToken);
        if (order is null) return null;

        var lines = await db.OrderLines.AsNoTracking()
            .Where(l => l.OrderId == query.OrderId)
            .OrderBy(l => l.Id)
            .ToListAsync(cancellationToken);

        return new OrderDetailsDto(ToDto(order), order.PaymentRef,
            lines.Select(l => new OrderLineDto(l.Sku, l.Quantity, Money.FromCents(l.UnitPriceCents), Money.FromCents(l.UnitPriceCents * l.Quantity))).ToArray());
    }

    public async Task<PagedResult<OrderSummaryDto>> HandleAsync(ListOrders query, CancellationToken cancellationToken)
    {
        var page = Math.Max(1, query.Page);
        var size = Math.Clamp(query.PageSize, 1, 200);
        var source = db.Orders.AsNoTracking();
        if (!string.IsNullOrWhiteSpace(query.Status)) source = source.Where(o => o.Status == query.Status);

        var total = await source.CountAsync(cancellationToken);
        var rows = await source.OrderByDescending(o => o.UpdatedAt)
            .Skip((page - 1) * size)
            .Take(size)
            .ToListAsync(cancellationToken);

        return new PagedResult<OrderSummaryDto>(rows.Select(ToDto).ToArray(), page, size, total);
    }

    public async Task<IReadOnlyList<DailySalesDto>> HandleAsync(GetDailySales query, CancellationToken cancellationToken)
    {
        var since = DateOnly.FromDateTime(DateTime.UtcNow).AddDays(-Math.Clamp(query.Days, 1, 365));
        var rows = await db.DailySales.AsNoTracking()
            .Where(d => d.Day >= since)
            .OrderBy(d => d.Day)
            .ToListAsync(cancellationToken);
        return rows.Select(d => new DailySalesDto(d.Day, d.PaidOrders, Money.FromCents(d.RevenueCents), d.Refunds, Money.FromCents(d.RefundedCents))).ToArray();
    }

    public async Task<ReadModelOverview> HandleAsync(GetReadModelOverview query, CancellationToken cancellationToken)
    {
        var byStatus = await db.Orders.AsNoTracking()
            .GroupBy(o => o.Status)
            .Select(g => new { Status = g.Key, Count = g.Count() })
            .ToListAsync(cancellationToken);
        var revenue = await db.DailySales.AsNoTracking().SumAsync(d => d.RevenueCents - d.RefundedCents, cancellationToken);
        var checkpoint = await db.Checkpoints.AsNoTracking()
            .Where(c => c.Projection == ProjectionEngine.CheckpointName)
            .Select(c => c.Position)
            .FirstOrDefaultAsync(cancellationToken);

        return new ReadModelOverview(byStatus.Sum(s => s.Count), byStatus.ToDictionary(s => s.Status, s => s.Count), Money.FromCents(revenue), checkpoint);
    }

    private static OrderSummaryDto ToDto(OrderSummaryView o) => new(o.OrderId, o.CustomerRef, o.Currency, o.Status, o.ItemCount,
        Money.FromCents(o.TotalCents), o.TrackingCode, o.FailureReason, o.Version,
        DateTime.SpecifyKind(o.CreatedAt, DateTimeKind.Utc), DateTime.SpecifyKind(o.UpdatedAt, DateTimeKind.Utc));
}
