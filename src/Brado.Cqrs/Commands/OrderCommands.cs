using Brado.Domain;
using Brado.EventStore.Repositories;

namespace Brado.Cqrs.Commands;

public sealed record OrderItemInput(string Sku, int Quantity, decimal UnitPrice);

public sealed record CreateOrder(string CustomerRef, IReadOnlyList<OrderItemInput> Items, string Currency = "BRL", bool Submit = false, Guid? OrderId = null)
    : ICommand<Guid>;

public sealed record AddOrderItem(Guid OrderId, string Sku, int Quantity, decimal UnitPrice) : ICommand<long>;

public sealed record SubmitOrder(Guid OrderId) : ICommand<long>;

public sealed record RecordPayment(Guid OrderId, string PaymentRef, decimal Amount) : ICommand<long>;

public sealed record RecordPaymentFailure(Guid OrderId, string Reason) : ICommand<long>;

public sealed record ReserveStock(Guid OrderId, string ReservationRef) : ICommand<long>;

public sealed record RecordStockFailure(Guid OrderId, string Reason) : ICommand<long>;

public sealed record RefundPayment(Guid OrderId, string RefundRef) : ICommand<long>;

public sealed record ShipOrder(Guid OrderId, string TrackingCode) : ICommand<long>;

public sealed record CancelOrder(Guid OrderId, string Reason) : ICommand<long>;

public sealed class OrderNotFoundException(Guid orderId) : Exception($"Pedido {orderId} não encontrado.")
{
    public Guid OrderId { get; } = orderId;
}

public sealed class OrderCommandHandlers(IAggregateRepository<Order> repository) :
    ICommandHandler<CreateOrder, Guid>,
    ICommandHandler<AddOrderItem, long>,
    ICommandHandler<SubmitOrder, long>,
    ICommandHandler<RecordPayment, long>,
    ICommandHandler<RecordPaymentFailure, long>,
    ICommandHandler<ReserveStock, long>,
    ICommandHandler<RecordStockFailure, long>,
    ICommandHandler<RefundPayment, long>,
    ICommandHandler<ShipOrder, long>,
    ICommandHandler<CancelOrder, long>
{
    public async Task<Guid> HandleAsync(CreateOrder command, CancellationToken cancellationToken)
    {
        var order = Order.Create(command.OrderId ?? Guid.CreateVersion7(), command.CustomerRef, command.Currency);
        foreach (var item in command.Items) order.AddItem(item.Sku, item.Quantity, item.UnitPrice);
        if (command.Submit) order.Submit();
        await repository.SaveAsync(order, cancellationToken);
        return order.Id;
    }

    public Task<long> HandleAsync(AddOrderItem c, CancellationToken ct) => Execute(c.OrderId, o => o.AddItem(c.Sku, c.Quantity, c.UnitPrice), ct);
    public Task<long> HandleAsync(SubmitOrder c, CancellationToken ct) => Execute(c.OrderId, o => o.Submit(), ct);
    public Task<long> HandleAsync(RecordPayment c, CancellationToken ct) => Execute(c.OrderId, o => o.RecordPayment(c.PaymentRef, c.Amount), ct);
    public Task<long> HandleAsync(RecordPaymentFailure c, CancellationToken ct) => Execute(c.OrderId, o => o.RecordPaymentFailure(c.Reason), ct);
    public Task<long> HandleAsync(ReserveStock c, CancellationToken ct) => Execute(c.OrderId, o => o.ReserveStock(c.ReservationRef), ct);
    public Task<long> HandleAsync(RecordStockFailure c, CancellationToken ct) => Execute(c.OrderId, o => o.RecordStockFailure(c.Reason), ct);
    public Task<long> HandleAsync(RefundPayment c, CancellationToken ct) => Execute(c.OrderId, o => o.RefundPayment(c.RefundRef), ct);
    public Task<long> HandleAsync(ShipOrder c, CancellationToken ct) => Execute(c.OrderId, o => o.Ship(c.TrackingCode), ct);
    public Task<long> HandleAsync(CancelOrder c, CancellationToken ct) => Execute(c.OrderId, o => o.Cancel(c.Reason), ct);

    private async Task<long> Execute(Guid orderId, Action<Order> action, CancellationToken cancellationToken)
    {
        var order = await repository.LoadAsync(orderId, cancellationToken) ?? throw new OrderNotFoundException(orderId);
        action(order);
        return await repository.SaveAsync(order, cancellationToken);
    }
}
