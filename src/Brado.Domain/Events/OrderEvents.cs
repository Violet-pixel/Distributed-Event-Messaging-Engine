namespace Brado.Domain.Events;

public interface IOrderEvent : IDomainEvent
{
    Guid OrderId { get; }
}

public sealed record OrderCreated(Guid OrderId, string CustomerRef, string Currency, DateTimeOffset CreatedAt) : IOrderEvent;

public sealed record OrderItemAdded(Guid OrderId, string Sku, int Quantity, decimal UnitPrice) : IOrderEvent;

public sealed record OrderSubmitted(Guid OrderId, decimal Total, int ItemCount, DateTimeOffset SubmittedAt) : IOrderEvent;

public sealed record PaymentProcessed(Guid OrderId, string PaymentRef, decimal Amount, DateTimeOffset ProcessedAt) : IOrderEvent;

public sealed record PaymentFailed(Guid OrderId, string Reason, DateTimeOffset FailedAt) : IOrderEvent;

public sealed record StockReserved(Guid OrderId, string ReservationRef, DateTimeOffset ReservedAt) : IOrderEvent;

public sealed record StockReservationFailed(Guid OrderId, string Reason, DateTimeOffset FailedAt) : IOrderEvent;

public sealed record PaymentRefunded(Guid OrderId, string RefundRef, decimal Amount, DateTimeOffset RefundedAt) : IOrderEvent;

public sealed record OrderShipped(Guid OrderId, string TrackingCode, DateTimeOffset ShippedAt) : IOrderEvent;

public sealed record OrderCancelled(Guid OrderId, string Reason, DateTimeOffset CancelledAt) : IOrderEvent;
