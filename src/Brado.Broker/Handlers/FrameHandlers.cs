using Brado.Broker.Server;
using Brado.Protocol;

namespace Brado.Broker.Handlers;

public interface IFrameHandler
{
    FrameType Type { get; }
    ValueTask HandleAsync(ClientConnection connection, Frame frame, CancellationToken cancellationToken);
}

public sealed class FrameHandlerRegistry
{
    private readonly Dictionary<FrameType, IFrameHandler> _handlers;

    public FrameHandlerRegistry(IEnumerable<IFrameHandler> handlers) =>
        _handlers = handlers.ToDictionary(h => h.Type);

    public async ValueTask HandleAsync(ClientConnection connection, Frame frame, CancellationToken cancellationToken)
    {
        if (!_handlers.TryGetValue(frame.Type, out var handler))
        {
            await connection.SendAsync(Frame.Error(frame.CorrelationId, $"Quadro {frame.Type} não é aceito pelo servidor."), cancellationToken);
            return;
        }

        try
        {
            await handler.HandleAsync(connection, frame, cancellationToken);
        }
        catch (ProtocolException ex)
        {
            await connection.SendAsync(Frame.Error(frame.CorrelationId, ex.Message), cancellationToken);
        }
    }
}

internal sealed class HelloHandler : IFrameHandler
{
    public FrameType Type => FrameType.Hello;

    public ValueTask HandleAsync(ClientConnection connection, Frame frame, CancellationToken cancellationToken)
    {
        connection.SetClientName(PayloadCodec.Utf8(frame.Payload));
        return connection.SendAsync(new Frame(FrameType.HelloAck, frame.CorrelationId, Payload: PayloadCodec.Int64(connection.Id)), cancellationToken);
    }
}

internal sealed class PingHandler : IFrameHandler
{
    public FrameType Type => FrameType.Ping;

    public ValueTask HandleAsync(ClientConnection connection, Frame frame, CancellationToken cancellationToken) =>
        connection.SendAsync(new Frame(FrameType.Pong, frame.CorrelationId), cancellationToken);
}

internal sealed class PublishHandler(MessageBroker broker) : IFrameHandler
{
    public FrameType Type => FrameType.Publish;

    public async ValueTask HandleAsync(ClientConnection connection, Frame frame, CancellationToken cancellationToken)
    {
        var offset = await broker.PublishOwnedAsync(frame.Topic, frame.Payload, cancellationToken);
        connection.CountPublished();
        await connection.SendAsync(new Frame(FrameType.PublishAck, frame.CorrelationId, frame.Topic, Offset: offset), cancellationToken);
    }
}

internal sealed class SubscribeHandler(IMessageBroker broker, Microsoft.Extensions.Options.IOptions<BrokerOptions> options) : IFrameHandler
{
    public FrameType Type => FrameType.Subscribe;

    public async ValueTask HandleAsync(ClientConnection connection, Frame frame, CancellationToken cancellationToken)
    {
        TopicName.EnsureValid(frame.Topic);
        var request = SubscribeRequest.Parse(frame.Payload.Span);
        TopicName.EnsureValid(request.Group);

        var prefetch = request.Prefetch is > 0 and <= 10_000 ? request.Prefetch : options.Value.DefaultPrefetch;
        var subscriptionId = broker.NextSubscriptionId();

        await connection.SendAsync(new Frame(FrameType.SubscribeAck, frame.CorrelationId, frame.Topic, PayloadCodec.Int64(subscriptionId)), cancellationToken);

        connection.AddSubscription(new SubscriptionBinding(frame.Topic, request.Group, subscriptionId));
        broker.Subscribe(frame.Topic, request.Group, new ConnectionSubscription(connection, subscriptionId, prefetch), request.StartOffset);
    }
}

internal sealed class UnsubscribeHandler(IMessageBroker broker) : IFrameHandler
{
    public FrameType Type => FrameType.Unsubscribe;

    public ValueTask HandleAsync(ClientConnection connection, Frame frame, CancellationToken cancellationToken)
    {
        var binding = connection.RemoveSubscription(frame.CorrelationId);
        if (binding is not null) broker.Unsubscribe(binding.Topic, binding.Group, binding.SubscriptionId);
        return ValueTask.CompletedTask;
    }
}

internal sealed class AckHandler(IMessageBroker broker) : IFrameHandler
{
    public FrameType Type => FrameType.Ack;

    public ValueTask HandleAsync(ClientConnection connection, Frame frame, CancellationToken cancellationToken)
    {
        var binding = connection.GetSubscription(frame.CorrelationId);
        if (binding is not null) broker.Ack(binding.Topic, binding.Group, binding.SubscriptionId, frame.Offset);
        return ValueTask.CompletedTask;
    }
}

internal sealed class NackHandler(IMessageBroker broker) : IFrameHandler
{
    public FrameType Type => FrameType.Nack;

    public ValueTask HandleAsync(ClientConnection connection, Frame frame, CancellationToken cancellationToken)
    {
        var binding = connection.GetSubscription(frame.CorrelationId);
        if (binding is not null)
            broker.Nack(binding.Topic, binding.Group, binding.SubscriptionId, frame.Offset, frame.Flags.HasFlag(FrameFlags.Requeue));
        return ValueTask.CompletedTask;
    }
}
