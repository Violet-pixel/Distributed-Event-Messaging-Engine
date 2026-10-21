using System.Text.Json;
using Brado.Domain.Contracts;
using Brado.EventStore.Sagas;
using Brado.Workers.Messaging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Brado.Workers.Sagas;

public sealed class SagaWorker(
    IServiceScopeFactory scopes,
    IMessageSubscriber subscriber,
    IInboxStore inbox,
    IOptions<SagaOptions> options,
    ILogger<SagaWorker> logger) : BackgroundService
{
    public const string ConsumerGroup = "saga.order-fulfillment";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!options.Value.Enabled)
        {
            logger.LogInformation("Saga desabilitada por configuração");
            return;
        }

        IAsyncDisposable? subscription = null;
        while (subscription is null && !stoppingToken.IsCancellationRequested)
        {
            try
            {
                subscription = await subscriber.SubscribeAsync(options.Value.Topic, ConsumerGroup, HandleAsync, prefetch: 16, stoppingToken);
                logger.LogInformation("Saga inscrita em {Topic} como {Group}", options.Value.Topic, ConsumerGroup);
            }
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
            {
                logger.LogWarning(ex, "Broker indisponível para a saga; tentando de novo em 3s");
                await Task.Delay(TimeSpan.FromSeconds(3), stoppingToken);
            }
        }

        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            if (subscription is not null) await subscription.DisposeAsync();
        }
    }

    internal async Task HandleAsync(ReadOnlyMemory<byte> payload, bool redelivered, CancellationToken cancellationToken)
    {
        EventEnvelope? envelope;
        try
        {
            envelope = JsonSerializer.Deserialize<EventEnvelope>(payload.Span, Json.Options);
        }
        catch (JsonException ex)
        {
            logger.LogWarning(ex, "Mensagem ignorada: não é um envelope de evento válido");
            return;
        }

        if (envelope is null || await inbox.HasProcessedAsync(ConsumerGroup, envelope.MessageId, cancellationToken)) return;

        await using var scope = scopes.CreateAsyncScope();
        var saga = scope.ServiceProvider.GetRequiredService<OrderFulfillmentSaga>();
        await saga.HandleAsync(envelope, cancellationToken);
        await inbox.MarkProcessedAsync(ConsumerGroup, envelope.MessageId, cancellationToken);
    }
}
