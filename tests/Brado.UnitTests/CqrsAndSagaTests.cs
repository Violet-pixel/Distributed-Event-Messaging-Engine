using System.Text.Json;
using Brado.Cqrs;
using Brado.Cqrs.Commands;
using Brado.Cqrs.Projections;
using Brado.Cqrs.Queries;
using Brado.Cqrs.ReadModel;
using Brado.Domain;
using Brado.Domain.Contracts;
using Brado.EventStore;
using Brado.EventStore.Outbox;
using Brado.EventStore.Repositories;
using Brado.Workers;
using Brado.Workers.Outbox;
using Brado.Workers.Sagas;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Brado.UnitTests;

internal sealed class FakePayments(bool succeed) : IPaymentGateway
{
    public Task<PaymentResult> ChargeAsync(Guid orderId, decimal amount, CancellationToken cancellationToken) =>
        Task.FromResult(succeed ? new PaymentResult(true, "PAY-TESTE", null) : new PaymentResult(false, null, "recusado"));

    public Task<string> RefundAsync(Guid orderId, CancellationToken cancellationToken) => Task.FromResult("REF-TESTE");
}

internal sealed class FakeInventory(bool succeed) : IInventoryService
{
    public Task<ReservationResult> ReserveAsync(Guid orderId, CancellationToken cancellationToken) =>
        Task.FromResult(succeed ? new ReservationResult(true, "RSV-TESTE", null) : new ReservationResult(false, null, "sem estoque"));
}

public sealed class CqrsAndSagaTests : IAsyncDisposable
{
    private readonly string _events = TestBroker.TempFile("es");
    private readonly string _read = TestBroker.TempFile("rm");

    private ServiceProvider Build(bool paymentOk = true, bool stockOk = true)
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["EventStore:Provider"] = "Sqlite",
            ["EventStore:ConnectionString"] = $"Data Source={_events}",
            ["ReadModel:Provider"] = "Sqlite",
            ["ReadModel:ConnectionString"] = $"Data Source={_read}"
        }).Build();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddBradoEventStore(configuration);
        services.AddBradoCqrs(configuration);
        services.AddSingleton<IPaymentGateway>(new FakePayments(paymentOk));
        services.AddSingleton<IInventoryService>(new FakeInventory(stockOk));
        services.AddScoped<OrderFulfillmentSaga>();
        return services.BuildServiceProvider();
    }

    private static async Task<Guid> CreateSubmittedOrderAsync(IServiceProvider provider)
    {
        await using var scope = provider.CreateAsyncScope();
        var commands = scope.ServiceProvider.GetRequiredService<ICommandDispatcher>();
        return await commands.SendAsync(new CreateOrder("CLI-500", [new OrderItemInput("LIV-1", 2, 50m)], Submit: true));
    }

    private static async Task PumpOutboxThroughSagaAsync(IServiceProvider provider)
    {
        var outbox = provider.GetRequiredService<IOutboxStore>();
        for (var round = 0; round < 20; round++)
        {
            var batch = await outbox.ClaimAsync("teste", 100, TimeSpan.FromMinutes(1));
            if (batch.Count == 0) return;
            foreach (var message in batch)
            {
                var envelope = JsonSerializer.Deserialize<EventEnvelope>(message.Payload, Json.Options)!;
                await using var scope = provider.CreateAsyncScope();
                await scope.ServiceProvider.GetRequiredService<OrderFulfillmentSaga>().HandleAsync(envelope, CancellationToken.None);
            }
            await outbox.MarkDispatchedAsync(batch.Select(m => m.Id).ToArray());
        }
    }

    private static async Task<Order> LoadAsync(IServiceProvider provider, Guid id) =>
        (await provider.GetRequiredService<IAggregateRepository<Order>>().LoadAsync(id))!;

    [Fact]
    public async Task Saga_ships_the_order_when_payment_and_stock_succeed()
    {
        await using var provider = Build();
        await provider.EnsureEventStoreSchemaAsync();
        var id = await CreateSubmittedOrderAsync(provider);

        await PumpOutboxThroughSagaAsync(provider);

        var order = await LoadAsync(provider, id);
        Assert.Equal(OrderStatus.Shipped, order.Status);
        Assert.Equal("PAY-TESTE", order.PaymentRef);
        Assert.StartsWith("BR", order.TrackingCode);
    }

    [Fact]
    public async Task Saga_compensates_with_refund_when_stock_fails()
    {
        await using var provider = Build(stockOk: false);
        await provider.EnsureEventStoreSchemaAsync();
        var id = await CreateSubmittedOrderAsync(provider);

        await PumpOutboxThroughSagaAsync(provider);

        var order = await LoadAsync(provider, id);
        Assert.Equal(OrderStatus.Cancelled, order.Status);
        Assert.True(order.Refunded);
        Assert.True(order.StockFailed);
    }

    [Fact]
    public async Task Saga_cancels_without_refund_when_payment_fails()
    {
        await using var provider = Build(paymentOk: false);
        await provider.EnsureEventStoreSchemaAsync();
        var id = await CreateSubmittedOrderAsync(provider);

        await PumpOutboxThroughSagaAsync(provider);

        var order = await LoadAsync(provider, id);
        Assert.Equal(OrderStatus.Cancelled, order.Status);
        Assert.Null(order.PaymentRef);
        Assert.False(order.Refunded);
    }

    [Fact]
    public async Task Projection_feeds_the_read_model_and_can_be_rebuilt()
    {
        await using var provider = Build();
        await provider.EnsureEventStoreSchemaAsync();
        await provider.EnsureReadModelAsync();
        var id = await CreateSubmittedOrderAsync(provider);
        await PumpOutboxThroughSagaAsync(provider);

        var engine = provider.GetRequiredService<ProjectionEngine>();
        await using (var scope = provider.CreateAsyncScope())
            await engine.ProcessBatchAsync(scope.ServiceProvider.GetRequiredService<ReadModelDbContext>(), 500, CancellationToken.None);

        async Task<OrderDetailsDto?> QueryAsync()
        {
            await using var scope = provider.CreateAsyncScope();
            return await scope.ServiceProvider.GetRequiredService<IQueryDispatcher>().QueryAsync(new GetOrder(id));
        }

        var before = await QueryAsync();
        Assert.Equal("Shipped", before!.Summary.Status);
        Assert.Equal(100m, before.Summary.Total);
        Assert.Single(before.Lines);

        await using (var scope = provider.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<ReadModelDbContext>();
            await engine.ResetAsync(db, CancellationToken.None);
        }
        Assert.Null(await QueryAsync());

        await using (var scope = provider.CreateAsyncScope())
            await engine.ProcessBatchAsync(scope.ServiceProvider.GetRequiredService<ReadModelDbContext>(), 500, CancellationToken.None);
        var after = await QueryAsync();
        Assert.Equal(before.Summary with { }, after!.Summary);
    }

    [Fact]
    public void Retry_delays_grow_exponentially_and_respect_the_cap()
    {
        var policy = new ExponentialBackoffRetryPolicy(Options.Create(new OutboxOptions
        {
            BaseRetryDelay = TimeSpan.FromSeconds(1),
            MaxRetryDelay = TimeSpan.FromSeconds(30)
        }));

        Assert.InRange(policy.NextDelay(1).TotalSeconds, 0.8, 1.2);
        Assert.InRange(policy.NextDelay(4).TotalSeconds, 6.4, 9.6);
        Assert.InRange(policy.NextDelay(50).TotalSeconds, 24, 36);
    }

    public ValueTask DisposeAsync()
    {
        SqliteConnection.ClearAllPools();
        foreach (var file in new[] { _events, _read }.SelectMany(p => new[] { p, p + "-wal", p + "-shm" }))
            if (File.Exists(file)) File.Delete(file);
        return ValueTask.CompletedTask;
    }
}
