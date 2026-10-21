using System.Collections.Concurrent;
using System.Text.Json;
using Brado.Cqrs;
using Brado.Cqrs.Commands;
using Brado.Domain;
using Brado.Domain.Contracts;
using Brado.Domain.Events;
using Brado.EventStore.Sagas;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Brado.Workers.Sagas;

public sealed record PaymentResult(bool Success, string? Reference, string? Error);

public sealed record ReservationResult(bool Success, string? Reference, string? Error);

public interface IPaymentGateway
{
    Task<PaymentResult> ChargeAsync(Guid orderId, decimal amount, CancellationToken cancellationToken);
    Task<string> RefundAsync(Guid orderId, CancellationToken cancellationToken);
}

public interface IInventoryService
{
    Task<ReservationResult> ReserveAsync(Guid orderId, CancellationToken cancellationToken);
}

public sealed class SimulatedPaymentGateway(IOptions<SagaOptions> options) : IPaymentGateway
{
    private readonly ConcurrentDictionary<Guid, PaymentResult> _charges = new();

    public async Task<PaymentResult> ChargeAsync(Guid orderId, decimal amount, CancellationToken cancellationToken)
    {
        if (_charges.TryGetValue(orderId, out var previous)) return previous;
        await Task.Delay(Jitter(options.Value.SimulatedLatencyMs), cancellationToken);
        var result = Random.Shared.NextDouble() < options.Value.PaymentFailureRate
            ? new PaymentResult(false, null, "Cartão recusado pelo emissor")
            : new PaymentResult(true, $"PAY-{Guid.NewGuid().ToString("N")[..10].ToUpperInvariant()}", null);
        return _charges.GetOrAdd(orderId, result);
    }

    public async Task<string> RefundAsync(Guid orderId, CancellationToken cancellationToken)
    {
        await Task.Delay(Jitter(options.Value.SimulatedLatencyMs), cancellationToken);
        return $"REF-{orderId.ToString("N")[..10].ToUpperInvariant()}";
    }

    internal static int Jitter(int baseMs) => baseMs <= 0 ? 0 : Random.Shared.Next(baseMs / 2, baseMs * 3 / 2 + 1);
}

public sealed class SimulatedInventoryService(IOptions<SagaOptions> options) : IInventoryService
{
    private readonly ConcurrentDictionary<Guid, ReservationResult> _reservations = new();

    public async Task<ReservationResult> ReserveAsync(Guid orderId, CancellationToken cancellationToken)
    {
        if (_reservations.TryGetValue(orderId, out var previous)) return previous;
        await Task.Delay(SimulatedPaymentGateway.Jitter(options.Value.SimulatedLatencyMs), cancellationToken);
        var result = Random.Shared.NextDouble() < options.Value.StockFailureRate
            ? new ReservationResult(false, null, "Item sem estoque no centro de distribuição")
            : new ReservationResult(true, $"RSV-{Guid.NewGuid().ToString("N")[..10].ToUpperInvariant()}", null);
        return _reservations.GetOrAdd(orderId, result);
    }
}

public static class SagaStates
{
    public const string AwaitingPayment = "AwaitingPayment";
    public const string ReservingStock = "ReservingStock";
    public const string Shipping = "Shipping";
    public const string Compensating = "Compensating";
    public const string Completed = "Completed";
    public const string Cancelled = "Cancelled";
    public const string Compensated = "Compensated";
}

public sealed class OrderFulfillmentSaga(
    ICommandDispatcher commands,
    IPaymentGateway payments,
    IInventoryService inventory,
    ISagaStore store,
    ILogger<OrderFulfillmentSaga> logger)
{
    public const string SagaType = "order-fulfillment";

    public async Task HandleAsync(EventEnvelope envelope, CancellationToken cancellationToken)
    {
        if (!envelope.Data.TryGetProperty("orderId", out var idProperty) || !idProperty.TryGetGuid(out var orderId)) return;
        var saga = await store.FindAsync(SagaType, orderId.ToString(), cancellationToken);

        switch (envelope.EventType)
        {
            case nameof(OrderSubmitted):
            {
                var e = envelope.ReadData<OrderSubmitted>();
                await SaveAsync(saga, orderId, SagaStates.AwaitingPayment, new { total = e.Total }, cancellationToken);
                var payment = await payments.ChargeAsync(orderId, e.Total, cancellationToken);
                if (payment.Success)
                    await SendAsync(new RecordPayment(orderId, payment.Reference!, e.Total), cancellationToken);
                else
                    await SendAsync(new RecordPaymentFailure(orderId, payment.Error ?? "Pagamento recusado"), cancellationToken);
                break;
            }
            case nameof(PaymentFailed):
            {
                var e = envelope.ReadData<PaymentFailed>();
                await SaveAsync(saga, orderId, SagaStates.Cancelled, new { reason = e.Reason }, cancellationToken);
                await SendAsync(new CancelOrder(orderId, $"Pagamento recusado: {e.Reason}"), cancellationToken);
                break;
            }
            case nameof(PaymentProcessed):
            {
                var e = envelope.ReadData<PaymentProcessed>();
                await SaveAsync(saga, orderId, SagaStates.ReservingStock, new { paymentRef = e.PaymentRef }, cancellationToken);
                var reservation = await inventory.ReserveAsync(orderId, cancellationToken);
                if (reservation.Success)
                    await SendAsync(new ReserveStock(orderId, reservation.Reference!), cancellationToken);
                else
                    await SendAsync(new RecordStockFailure(orderId, reservation.Error ?? "Sem estoque"), cancellationToken);
                break;
            }
            case nameof(StockReserved):
            {
                await SaveAsync(saga, orderId, SagaStates.Shipping, new { }, cancellationToken);
                var tracking = $"BR{Random.Shared.Next(100_000_000, 999_999_999)}BD";
                await SendAsync(new ShipOrder(orderId, tracking), cancellationToken);
                break;
            }
            case nameof(StockReservationFailed):
            {
                await SaveAsync(saga, orderId, SagaStates.Compensating, new { step = "refund" }, cancellationToken);
                var refundRef = await payments.RefundAsync(orderId, cancellationToken);
                await SendAsync(new RefundPayment(orderId, refundRef), cancellationToken);
                break;
            }
            case nameof(PaymentRefunded):
            {
                await SaveAsync(saga, orderId, SagaStates.Compensating, new { step = "cancel" }, cancellationToken);
                await SendAsync(new CancelOrder(orderId, "Estoque indisponível — pagamento estornado"), cancellationToken);
                break;
            }
            case nameof(OrderShipped):
                await SaveAsync(saga, orderId, SagaStates.Completed, new { }, cancellationToken);
                break;
            case nameof(OrderCancelled):
                var final = saga?.State == SagaStates.Compensating ? SagaStates.Compensated : SagaStates.Cancelled;
                await SaveAsync(saga, orderId, final, new { }, cancellationToken);
                break;
        }
    }

    private Task SaveAsync(SagaInstance? current, Guid orderId, string state, object data, CancellationToken cancellationToken)
    {
        var now = DateTimeOffset.UtcNow;
        var saga = current is null
            ? new SagaInstance(Guid.CreateVersion7(), SagaType, orderId.ToString(), state, JsonSerializer.Serialize(data, Json.Options), 1, now, now)
            : current with { State = state, Data = JsonSerializer.Serialize(data, Json.Options), UpdatedAt = now };
        return store.SaveAsync(saga, cancellationToken);
    }

    private async Task SendAsync<T>(ICommand<T> command, CancellationToken cancellationToken)
    {
        try
        {
            await commands.SendAsync(command, cancellationToken);
        }
        catch (DomainException ex)
        {
            logger.LogDebug("Comando {Command} ignorado (já aplicado): {Message}", typeof(T).Name, ex.Message);
        }
    }
}
