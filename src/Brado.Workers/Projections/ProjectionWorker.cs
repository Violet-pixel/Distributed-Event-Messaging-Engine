using Brado.Cqrs.Projections;
using Brado.Cqrs.ReadModel;
using Brado.EventStore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Brado.Workers.Projections;

public sealed class ProjectionWorker(
    IServiceScopeFactory scopes,
    ProjectionEngine engine,
    EventAppendedSignal signal,
    IOptions<ProjectionOptions> options,
    ILogger<ProjectionWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var wakeUp = signal.Subscribe();
        while (!stoppingToken.IsCancellationRequested)
        {
            var processed = 0;
            try
            {
                await using var scope = scopes.CreateAsyncScope();
                var db = scope.ServiceProvider.GetRequiredService<ReadModelDbContext>();
                processed = await engine.ProcessBatchAsync(db, options.Value.BatchSize, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Falha ao projetar eventos a partir da posição {Position}", engine.Position);
                await wakeUp.WaitAsync(TimeSpan.FromSeconds(2), stoppingToken);
                continue;
            }

            if (processed < options.Value.BatchSize)
                await wakeUp.WaitAsync(options.Value.PollingInterval, stoppingToken);
        }
    }
}
