using System.Text;
using System.Text.Json;
using Brado.Broker;
using Brado.Broker.Metrics;
using Brado.Cqrs;
using Brado.Cqrs.Commands;
using Brado.Cqrs.Projections;
using Brado.Cqrs.Queries;
using Brado.Cqrs.ReadModel;
using Brado.Domain;
using Brado.EventStore;
using Brado.EventStore.Outbox;
using Brado.EventStore.Sagas;
using Brado.Protocol;
using Brado.Workers.Outbox;
using Microsoft.AspNetCore.Diagnostics;
using Microsoft.Extensions.Options;

namespace Brado.Server.Api;

public sealed record CreateOrderRequest(string CustomerRef, IReadOnlyList<OrderItemInput>? Items, string? Currency, bool Submit = false);
public sealed record AddItemRequest(string Sku, int Quantity, decimal UnitPrice);
public sealed record CancelRequest(string? Reason);

public sealed record OrderStateView(string Status, decimal Total, int Lines, string? PaymentRef, string? TrackingCode, bool Refunded, string? CancellationReason);
public sealed record ReplayStep(long Version, long Position, string EventType, DateTimeOffset OccurredAt, JsonElement Data, OrderStateView? State);
public sealed record MessagePreview(long Offset, DateTimeOffset Timestamp, int Size, string Preview);

public static class BradoApi
{
    public static IEndpointRouteBuilder MapBradoApi(this IEndpointRouteBuilder app)
    {
        var api = app.MapGroup("/api");

        api.MapPost("/orders", async (CreateOrderRequest request, ICommandDispatcher commands, CancellationToken ct) =>
        {
            var id = await commands.SendAsync(new CreateOrder(request.CustomerRef, request.Items ?? [], request.Currency ?? "BRL", request.Submit), ct);
            return Results.Created($"/api/orders/{id}", new { orderId = id });
        });

        api.MapPost("/orders/{id:guid}/items", async (Guid id, AddItemRequest request, ICommandDispatcher commands, CancellationToken ct) =>
            Results.Ok(new { version = await commands.SendAsync(new AddOrderItem(id, request.Sku, request.Quantity, request.UnitPrice), ct) }));

        api.MapPost("/orders/{id:guid}/submit", async (Guid id, ICommandDispatcher commands, CancellationToken ct) =>
            Results.Ok(new { version = await commands.SendAsync(new SubmitOrder(id), ct) }));

        api.MapPost("/orders/{id:guid}/cancel", async (Guid id, CancelRequest? request, ICommandDispatcher commands, CancellationToken ct) =>
            Results.Ok(new { version = await commands.SendAsync(new CancelOrder(id, request?.Reason ?? "Cancelado pelo operador"), ct) }));

        api.MapGet("/orders", (string? status, int? page, int? pageSize, IQueryDispatcher queries, CancellationToken ct) =>
            queries.QueryAsync(new ListOrders(status, page ?? 1, pageSize ?? 20), ct));

        api.MapGet("/orders/{id:guid}", async (Guid id, IQueryDispatcher queries, CancellationToken ct) =>
            await queries.QueryAsync(new GetOrder(id), ct) is { } order ? Results.Ok(order) : Results.NotFound());

        api.MapGet("/sales/daily", (int? days, IQueryDispatcher queries, CancellationToken ct) =>
            queries.QueryAsync(new GetDailySales(days ?? 14), ct));

        api.MapGet("/read-model", (IQueryDispatcher queries, CancellationToken ct) =>
            queries.QueryAsync(new GetReadModelOverview(), ct));

        api.MapPost("/projections/rebuild", async (ProjectionEngine engine, ReadModelDbContext db, EventAppendedSignal signal, CancellationToken ct) =>
        {
            await engine.ResetAsync(db, ct);
            signal.Notify();
            return Results.Accepted(value: new { message = "Replay completo iniciado" });
        });

        api.MapGet("/streams", (int? skip, int? take, string? search, IEventStore store, CancellationToken ct) =>
            store.ListStreamsAsync(skip ?? 0, take ?? 30, search, ct));

        api.MapGet("/streams/{streamId}/events", (string streamId, IEventStore store, CancellationToken ct) =>
            store.ReadStreamAsync(streamId, 0, ct));

        api.MapGet("/streams/{streamId}/replay", async (string streamId, long? toVersion, IEventStore store, EventTypeRegistry registry, CancellationToken ct) =>
        {
            var events = await store.ReadStreamAsync(streamId, 0, ct);
            if (events.Count == 0) return Results.NotFound();

            var order = streamId.StartsWith("order-", StringComparison.Ordinal) ? new Order() : null;
            var steps = new List<ReplayStep>();
            foreach (var e in events.Where(e => toVersion is null || e.StreamVersion <= toVersion))
            {
                OrderStateView? state = null;
                if (order is not null && registry.TryDeserialize(e.EventType, e.Payload, out var domainEvent) && domainEvent is not null)
                {
                    order.LoadFromHistory([domainEvent]);
                    state = new OrderStateView(order.Status.ToString(), order.Total, order.Lines.Count, order.PaymentRef,
                        order.TrackingCode, order.Refunded, order.CancellationReason);
                }

                using var data = JsonDocument.Parse(e.Payload);
                steps.Add(new ReplayStep(e.StreamVersion, e.GlobalPosition, e.EventType, e.OccurredAt, data.RootElement.Clone(), state));
            }
            return Results.Ok(new { streamId, steps });
        });

        api.MapGet("/events", (long? after, int? take, IEventStore store, CancellationToken ct) =>
            store.ReadAllAsync(after ?? 0, take ?? 50, ct));

        api.MapGet("/events/stats", (IEventStore store, CancellationToken ct) => store.GetStatsAsync(ct));

        api.MapGet("/broker", (BrokerMonitor monitor) => monitor.Snapshot());

        api.MapGet("/broker/topics/{topic}/messages", (string topic, long? from, int? take, BrokerMonitor monitor) =>
            monitor.Peek(topic, from ?? 0, take ?? 50).Select(m => new MessagePreview(m.Offset, m.Timestamp, m.Payload.Length,
                Encoding.UTF8.GetString(m.Payload.Span[..Math.Min(m.Payload.Length, 400)]))));

        api.MapPost("/broker/topics/{topic}/publish", async (string topic, HttpRequest request, MessageBroker broker, CancellationToken ct) =>
        {
            using var buffer = new MemoryStream();
            await request.Body.CopyToAsync(buffer, ct);
            var offset = await broker.PublishOwnedAsync(topic, buffer.ToArray(), ct);
            return Results.Ok(new { topic, offset });
        });

        api.MapGet("/outbox", async (IOutboxStore store, OutboxMetrics metrics, CancellationToken ct) =>
            new { stats = await store.GetStatsAsync(ct), metrics.Dispatched, metrics.Failed, metrics.Batches, metrics.LastDispatchAt, metrics.LastError });

        api.MapGet("/sagas", (int? take, ISagaStore store, CancellationToken ct) => store.ListRecentAsync(take ?? 20, ct));

        api.MapGet("/overview", (DashboardService dashboard, CancellationToken ct) => dashboard.BuildAsync(ct));

        api.MapGet("/stream", async (HttpContext context, DashboardService dashboard, IOptions<Microsoft.AspNetCore.Http.Json.JsonOptions> json) =>
        {
            var ct = context.RequestAborted;
            context.Response.Headers.ContentType = "text/event-stream";
            context.Response.Headers.CacheControl = "no-cache";
            context.Response.Headers["X-Accel-Buffering"] = "no";

            try
            {
                using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
                do
                {
                    var snapshot = await dashboard.BuildAsync(ct);
                    await context.Response.WriteAsync($"event: snapshot\ndata: {JsonSerializer.Serialize(snapshot, json.Value.SerializerOptions)}\n\n", ct);
                    await context.Response.Body.FlushAsync(ct);
                }
                while (await timer.WaitForNextTickAsync(ct));
            }
            catch (OperationCanceledException)
            {
            }
        });

        api.MapPost("/simulate", (int? orders, int? messages, TrafficSimulator simulator) =>
        {
            simulator.Burst(Math.Clamp(orders ?? 25, 0, 2000), Math.Clamp(messages ?? 600, 0, 200_000));
            return Results.Accepted(value: new { message = "Simulação iniciada" });
        });

        app.MapGet("/health", () => Results.Ok(new { status = "ok", at = DateTimeOffset.UtcNow }));
        return app;
    }

    public static IApplicationBuilder UseBradoErrors(this IApplicationBuilder app) =>
        app.UseExceptionHandler(handler => handler.Run(async context =>
        {
            var error = context.Features.Get<IExceptionHandlerFeature>()?.Error;
            var (status, title) = error switch
            {
                DomainException => (StatusCodes.Status422UnprocessableEntity, "Regra de negócio violada"),
                OrderNotFoundException => (StatusCodes.Status404NotFound, "Pedido não encontrado"),
                ConcurrencyException => (StatusCodes.Status409Conflict, "Conflito de concorrência"),
                ProtocolException or BadHttpRequestException or ArgumentException => (StatusCodes.Status400BadRequest, "Requisição inválida"),
                _ => (StatusCodes.Status500InternalServerError, "Erro interno")
            };

            context.Response.StatusCode = status;
            await context.Response.WriteAsJsonAsync(new { status, title, detail = status == 500 ? null : error?.Message });
        }));
}
