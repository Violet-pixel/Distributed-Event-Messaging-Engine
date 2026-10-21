using Brado.Domain.Events;

namespace Brado.Domain;

public enum OrderStatus
{
    Draft,
    Submitted,
    Paid,
    StockReserved,
    Shipped,
    Cancelled
}

public sealed record OrderLine(string Sku, int Quantity, decimal UnitPrice)
{
    public decimal Subtotal => Quantity * UnitPrice;
}

public sealed record OrderSnapshot(
    Guid Id,
    string CustomerRef,
    string Currency,
    OrderStatus Status,
    IReadOnlyList<OrderLine> Lines,
    string? PaymentRef,
    bool Refunded,
    bool StockFailed,
    string? TrackingCode,
    string? CancellationReason);

public sealed class Order : AggregateRoot, ISnapshotable
{
    private readonly List<OrderLine> _lines = [];

    public override string StreamType => "order";

    public string CustomerRef { get; private set; } = string.Empty;
    public string Currency { get; private set; } = "BRL";
    public OrderStatus Status { get; private set; }
    public IReadOnlyList<OrderLine> Lines => _lines;
    public decimal Total => _lines.Sum(l => l.Subtotal);
    public string? PaymentRef { get; private set; }
    public bool Refunded { get; private set; }
    public bool StockFailed { get; private set; }
    public string? TrackingCode { get; private set; }
    public string? CancellationReason { get; private set; }

    public static Order Create(Guid id, string customerRef, string currency = "BRL")
    {
        if (id == Guid.Empty) throw new DomainException("O id do pedido é obrigatório.");
        if (string.IsNullOrWhiteSpace(customerRef)) throw new DomainException("A referência do cliente é obrigatória.");
        if (currency.Length != 3) throw new DomainException("Moeda deve ter 3 letras (ISO 4217).");

        var order = new Order();
        order.Raise(new OrderCreated(id, customerRef.Trim(), currency.ToUpperInvariant(), DateTimeOffset.UtcNow));
        return order;
    }

    public void AddItem(string sku, int quantity, decimal unitPrice)
    {
        EnsureStatus(OrderStatus.Draft, "adicionar itens");
        if (string.IsNullOrWhiteSpace(sku)) throw new DomainException("SKU é obrigatório.");
        if (quantity <= 0) throw new DomainException("Quantidade deve ser maior que zero.");
        if (unitPrice < 0) throw new DomainException("Preço não pode ser negativo.");
        Raise(new OrderItemAdded(Id, sku.Trim().ToUpperInvariant(), quantity, unitPrice));
    }

    public void Submit()
    {
        EnsureStatus(OrderStatus.Draft, "enviar");
        if (_lines.Count == 0) throw new DomainException("Pedido sem itens não pode ser enviado.");
        Raise(new OrderSubmitted(Id, Total, _lines.Count, DateTimeOffset.UtcNow));
    }

    public void RecordPayment(string paymentRef, decimal amount)
    {
        EnsureStatus(OrderStatus.Submitted, "registrar pagamento");
        if (amount != Total) throw new DomainException($"Valor pago ({amount}) difere do total ({Total}).");
        Raise(new PaymentProcessed(Id, paymentRef, amount, DateTimeOffset.UtcNow));
    }

    public void RecordPaymentFailure(string reason)
    {
        EnsureStatus(OrderStatus.Submitted, "registrar falha de pagamento");
        Raise(new PaymentFailed(Id, reason, DateTimeOffset.UtcNow));
    }

    public void ReserveStock(string reservationRef)
    {
        EnsureStatus(OrderStatus.Paid, "reservar estoque");
        if (StockFailed) throw new DomainException("Reserva de estoque já falhou para este pedido.");
        Raise(new StockReserved(Id, reservationRef, DateTimeOffset.UtcNow));
    }

    public void RecordStockFailure(string reason)
    {
        EnsureStatus(OrderStatus.Paid, "registrar falha de estoque");
        if (StockFailed) throw new DomainException("Falha de estoque já registrada.");
        Raise(new StockReservationFailed(Id, reason, DateTimeOffset.UtcNow));
    }

    public void RefundPayment(string refundRef)
    {
        if (PaymentRef is null) throw new DomainException("Não há pagamento para estornar.");
        if (Refunded) throw new DomainException("Pagamento já estornado.");
        if (Status is OrderStatus.Shipped or OrderStatus.Cancelled)
            throw new DomainException($"Não é possível estornar um pedido {Status}.");
        Raise(new PaymentRefunded(Id, refundRef, Total, DateTimeOffset.UtcNow));
    }

    public void Ship(string trackingCode)
    {
        EnsureStatus(OrderStatus.StockReserved, "enviar para entrega");
        if (string.IsNullOrWhiteSpace(trackingCode)) throw new DomainException("Código de rastreio é obrigatório.");
        Raise(new OrderShipped(Id, trackingCode, DateTimeOffset.UtcNow));
    }

    public void Cancel(string reason)
    {
        if (Status is OrderStatus.Shipped or OrderStatus.Cancelled)
            throw new DomainException($"Não é possível cancelar um pedido {Status}.");
        if (PaymentRef is not null && !Refunded)
            throw new DomainException("Estorne o pagamento antes de cancelar (compensação da saga).");
        Raise(new OrderCancelled(Id, reason, DateTimeOffset.UtcNow));
    }

    protected override void Apply(IDomainEvent @event)
    {
        switch (@event)
        {
            case OrderCreated e:
                Id = e.OrderId;
                CustomerRef = e.CustomerRef;
                Currency = e.Currency;
                Status = OrderStatus.Draft;
                break;
            case OrderItemAdded e:
                _lines.Add(new OrderLine(e.Sku, e.Quantity, e.UnitPrice));
                break;
            case OrderSubmitted:
                Status = OrderStatus.Submitted;
                break;
            case PaymentProcessed e:
                PaymentRef = e.PaymentRef;
                Status = OrderStatus.Paid;
                break;
            case PaymentFailed:
                break;
            case StockReserved:
                Status = OrderStatus.StockReserved;
                break;
            case StockReservationFailed:
                StockFailed = true;
                break;
            case PaymentRefunded:
                Refunded = true;
                break;
            case OrderShipped e:
                TrackingCode = e.TrackingCode;
                Status = OrderStatus.Shipped;
                break;
            case OrderCancelled e:
                CancellationReason = e.Reason;
                Status = OrderStatus.Cancelled;
                break;
        }
    }

    private void EnsureStatus(OrderStatus expected, string action)
    {
        if (Status != expected)
            throw new DomainException($"Não é possível {action}: pedido está {Status}, esperado {expected}.");
    }

    Type ISnapshotable.SnapshotType => typeof(OrderSnapshot);

    object ISnapshotable.TakeSnapshot() =>
        new OrderSnapshot(Id, CustomerRef, Currency, Status, _lines.ToArray(), PaymentRef, Refunded, StockFailed, TrackingCode, CancellationReason);

    void ISnapshotable.RestoreSnapshot(object snapshot, long version)
    {
        var s = (OrderSnapshot)snapshot;
        Id = s.Id;
        CustomerRef = s.CustomerRef;
        Currency = s.Currency;
        Status = s.Status;
        _lines.Clear();
        _lines.AddRange(s.Lines);
        PaymentRef = s.PaymentRef;
        Refunded = s.Refunded;
        StockFailed = s.StockFailed;
        TrackingCode = s.TrackingCode;
        CancellationReason = s.CancellationReason;
        RestoreVersion(version);
    }
}
