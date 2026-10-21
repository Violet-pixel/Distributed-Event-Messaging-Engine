using System.Text;
using Brado.EventStore;
using Brado.EventStore.Outbox;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Brado.Workers.Outbox;

public interface IRetryPolicy
{
    TimeSpan NextDelay(int attempt);
}

public sealed class ExponentialBackoffRetryPolicy(IOptions<OutboxOptions> options) : IRetryPolicy
{
    public TimeSpan NextDelay(int attempt)
    {
        var o = options.Value;
        var exponent = Math.Min(Math.Max(0, attempt - 1), 20);
        var raw = Math.Min(o.MaxRetryDelay.TotalMilliseconds, o.BaseRetryDelay.TotalMilliseconds * Math.Pow(2, exponent));
        var jitter = 0.8 + Random.Shared.NextDouble() * 0.4;
        return TimeSpan.FromMilliseconds(raw * jitter);
    }
}

public sealed class OutboxMetrics
{
    private long _dispatched, _failed, _batches;

    public long Dispatched => Interlocked.Read(ref _dispatched);
    public long Failed => Interlocked.Read(ref _failed);
    public long Batches => Interlocked.Read(ref _batches);
    public DateTimeOffset? LastDispatchAt { get; private set; }
    public string? LastError { get; private set; }

    internal void Batch() => Interlocked.Increment(ref _batches);

    internal void Success(int count)
    {
        Interlocked.Add(ref _dispatched, count);
        LastDispatchAt = DateTimeOffset.UtcNow;
    }

    internal void Failure(string error)
    {
        Interlocked.Increment(ref _failed);
        LastError = error;
    }
}

public sealed class OutboxDispatcherService(
    IOutboxStore store,
    IMessagePublisher publisher,
    EventAppendedSignal signal,
    IRetryPolicy retryPolicy,
    OutboxMetrics metrics,
    IOptions<OutboxOptions> options,
    ILogger<OutboxDispatcherService> logger) : BackgroundService
{
    private readonly string _workerId = $"outbox-{Environment.ProcessId}-{Guid.NewGuid().ToString("N")[..8]}";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var wakeUp = signal.Subscribe();
        var nextPurge = DateTimeOffset.UtcNow.AddMinutes(1);
        logger.LogInformation("Outbox dispatcher {Worker} iniciado", _workerId);

        while (!stoppingToken.IsCancellationRequested)
        {
            var claimed = 0;
            try
            {
                claimed = await DispatchBatchAsync(stoppingToken);

                if (DateTimeOffset.UtcNow >= nextPurge)
                {
                    var purged = await store.PurgeDispatchedAsync(DateTimeOffset.UtcNow - options.Value.DispatchedRetention, stoppingToken);
                    if (purged > 0) logger.LogInformation("Outbox: {Count} mensagens antigas removidas", purged);
                    nextPurge = DateTimeOffset.UtcNow.AddHours(1);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Erro no ciclo do outbox");
                metrics.Failure(ex.Message);
            }

            if (claimed < options.Value.BatchSize)
                await wakeUp.WaitAsync(options.Value.PollingInterval, stoppingToken);
        }
    }

    public async Task<int> DispatchBatchAsync(CancellationToken cancellationToken)
    {
        var o = options.Value;
        var batch = await store.ClaimAsync(_workerId, o.BatchSize, o.Lease, cancellationToken);
        if (batch.Count == 0) return 0;

        metrics.Batch();
        var delivered = new List<long>(batch.Count);
        var failures = new List<OutboxFailure>();

        foreach (var message in batch)
        {
            try
            {
                await publisher.PublishAsync(message.Topic, Encoding.UTF8.GetBytes(message.Payload), cancellationToken);
                delivered.Add(message.Id);
            }
            catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
            {
                var delay = retryPolicy.NextDelay((int)message.Attempts + 1);
                failures.Add(new OutboxFailure(message.Id, ex.Message, DateTimeOffset.UtcNow + delay));
                metrics.Failure(ex.Message);
                logger.LogWarning("Falha ao publicar {MessageId} em {Topic} (tentativa {Attempt}); nova tentativa em {Delay}",
                    message.MessageId, message.Topic, message.Attempts + 1, delay);
            }
        }

        await store.MarkDispatchedAsync(delivered, CancellationToken.None);
        await store.MarkFailedAsync(failures, CancellationToken.None);
        metrics.Success(delivered.Count);
        return batch.Count;
    }
}
