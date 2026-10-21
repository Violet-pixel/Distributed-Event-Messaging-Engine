using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace Brado.Broker;

internal sealed class LocalSubscription : IDeliveryTarget, IAsyncDisposable
{
    private readonly IMessageBroker _broker;
    private readonly string _topic;
    private readonly string _group;
    private readonly Func<Delivery, CancellationToken, ValueTask> _handler;
    private readonly ILogger _logger;
    private readonly Channel<Delivery> _channel = Channel.CreateUnbounded<Delivery>(new UnboundedChannelOptions { SingleReader = true });
    private readonly CancellationTokenSource _cts = new();
    private Task _loop = Task.CompletedTask;

    public LocalSubscription(IMessageBroker broker, string topic, string group, long subscriptionId, int prefetch,
        Func<Delivery, CancellationToken, ValueTask> handler, ILogger logger)
    {
        _broker = broker;
        _topic = topic;
        _group = group;
        SubscriptionId = subscriptionId;
        Prefetch = prefetch;
        _handler = handler;
        _logger = logger;
    }

    public long SubscriptionId { get; }
    public int Prefetch { get; }
    public string Description => $"local:{_group}";

    public void Start() => _loop = Task.Run(() => ConsumeAsync(_cts.Token));

    public bool TryDeliver(string topic, string group, StoredMessage message, int attempt) =>
        _channel.Writer.TryWrite(new Delivery(topic, group, SubscriptionId, message.Offset, message.Payload, attempt, message.Timestamp));

    private async Task ConsumeAsync(CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var delivery in _channel.Reader.ReadAllAsync(cancellationToken))
            {
                try
                {
                    await _handler(delivery, cancellationToken);
                    _broker.Ack(_topic, _group, SubscriptionId, delivery.Offset);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Handler local de {Group} falhou no offset {Offset} (tentativa {Attempt})",
                        _group, delivery.Offset, delivery.Attempt);
                    _broker.Nack(_topic, _group, SubscriptionId, delivery.Offset, requeue: true);
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    public async ValueTask DisposeAsync()
    {
        _broker.Unsubscribe(_topic, _group, SubscriptionId);
        _channel.Writer.TryComplete();
        await _cts.CancelAsync();
        try { await _loop; } catch (OperationCanceledException) { }
        _cts.Dispose();
    }
}
