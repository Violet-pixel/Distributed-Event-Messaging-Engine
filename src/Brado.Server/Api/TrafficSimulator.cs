using System.Text.Json;
using Brado.Broker;
using Brado.Cqrs;
using Brado.Cqrs.Commands;
using Brado.Protocol;
using Microsoft.Extensions.Options;

namespace Brado.Server.Api;

public sealed class SimulationOptions
{
    public const string Section = "Simulation";

    public bool Ambient { get; set; }
    public int AmbientOrdersPerMinute { get; set; } = 12;
    public int AmbientMessagesPerSecond { get; set; } = 40;
}

public sealed class TrafficSimulator(
    IServiceScopeFactory scopes,
    MessageBroker broker,
    IOptions<SimulationOptions> options,
    ILogger<TrafficSimulator> logger) : BackgroundService
{
    private static readonly (string Sku, decimal Price)[] Catalog =
    [
        ("LIV-ARQ-001", 129.90m), ("LIV-DDD-002", 189.00m), ("LIV-ALG-003", 99.50m), ("CUR-NET-010", 349.00m),
        ("CUR-SQL-011", 219.90m), ("KIT-CAF-020", 59.90m), ("CAN-PRT-030", 24.90m), ("FON-BLT-040", 279.00m)
    ];

    private static readonly string[] Topics = ["telemetria.sensores", "notificacoes.email", "auditoria.acessos"];

    private readonly SemaphoreSlim _consumersGate = new(1, 1);
    private readonly List<IAsyncDisposable> _consumers = [];
    private CancellationToken _stopping;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _stopping = stoppingToken;
        if (!options.Value.Ambient) return;

        await EnsureConsumersAsync();
        logger.LogInformation("Tráfego ambiente ligado");
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        var tick = 0;
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                tick++;
                var perSecond = options.Value.AmbientMessagesPerSecond;
                var wave = 0.6 + 0.4 * Math.Sin(tick / 9.0);
                await PublishTelemetryAsync((int)(perSecond * wave), stoppingToken);

                var everySeconds = Math.Max(1, 60 / Math.Max(1, options.Value.AmbientOrdersPerMinute));
                if (tick % everySeconds == 0) await CreateOrderAsync(stoppingToken);
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    public void Burst(int orders, int messages)
    {
        _ = Task.Run(async () =>
        {
            try
            {
                await EnsureConsumersAsync();
                var ordersTask = Task.Run(async () =>
                {
                    for (var i = 0; i < orders && !_stopping.IsCancellationRequested; i++)
                    {
                        await CreateOrderAsync(_stopping);
                        await Task.Delay(Random.Shared.Next(30, 120), _stopping);
                    }
                });

                var sent = 0;
                while (sent < messages && !_stopping.IsCancellationRequested)
                {
                    var chunk = Math.Min(messages - sent, Random.Shared.Next(40, 160));
                    await PublishTelemetryAsync(chunk, _stopping);
                    sent += chunk;
                    await Task.Delay(100, _stopping);
                }
                await ordersTask;
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Falha na simulação");
            }
        });
    }

    private async Task CreateOrderAsync(CancellationToken cancellationToken)
    {
        var items = Enumerable.Range(0, Random.Shared.Next(1, 4))
            .Select(_ => Catalog[Random.Shared.Next(Catalog.Length)])
            .Select(p => new OrderItemInput(p.Sku, Random.Shared.Next(1, 4), p.Price))
            .ToArray();

        await using var scope = scopes.CreateAsyncScope();
        var commands = scope.ServiceProvider.GetRequiredService<ICommandDispatcher>();
        await commands.SendAsync(new CreateOrder($"CLI-{Random.Shared.Next(1000, 9999)}", items, "BRL", Submit: true), cancellationToken);
    }

    private async Task PublishTelemetryAsync(int count, CancellationToken cancellationToken)
    {
        for (var i = 0; i < count; i++)
        {
            var topic = Topics[Random.Shared.Next(Topics.Length)];
            var payload = JsonSerializer.SerializeToUtf8Bytes(new
            {
                id = Guid.NewGuid(),
                source = $"node-{Random.Shared.Next(1, 9)}",
                value = Math.Round(Random.Shared.NextDouble() * 100, 2),
                at = DateTimeOffset.UtcNow
            });
            await broker.PublishOwnedAsync(topic, payload, cancellationToken);
        }
    }

    private async Task EnsureConsumersAsync()
    {
        await _consumersGate.WaitAsync();
        try
        {
            if (_consumers.Count > 0) return;
            await broker.InitializeAsync();

            _consumers.Add(broker.SubscribeLocal("telemetria.sensores", "analytics", (_, ct) => Work(ct, 1, 0.01), 128, StartPosition.Latest));
            _consumers.Add(broker.SubscribeLocal("telemetria.sensores", "arquivo-frio", (_, ct) => Work(ct, 3, 0), 64, StartPosition.Latest));
            _consumers.Add(broker.SubscribeLocal("notificacoes.email", "mailer", (_, ct) => Work(ct, 6, 0.03), 32, StartPosition.Latest));
            _consumers.Add(broker.SubscribeLocal("auditoria.acessos", "siem", (_, ct) => Work(ct, 2, 0.005), 64, StartPosition.Latest));
            _consumers.Add(broker.SubscribeLocal("orders.events", "notificador-pedidos", (_, ct) => Work(ct, 4, 0), 32, StartPosition.Latest));
        }
        finally
        {
            _consumersGate.Release();
        }
    }

    private static async ValueTask Work(CancellationToken cancellationToken, int maxDelayMs, double failureRate)
    {
        if (maxDelayMs > 0) await Task.Delay(Random.Shared.Next(0, maxDelayMs + 1), cancellationToken);
        if (failureRate > 0 && Random.Shared.NextDouble() < failureRate)
            throw new InvalidOperationException("Falha simulada no consumidor");
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken);
        foreach (var consumer in _consumers) await consumer.DisposeAsync();
    }
}
