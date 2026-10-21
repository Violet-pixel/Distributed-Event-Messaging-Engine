using System.Net.Sockets;
using System.Threading.Channels;
using Brado.Broker.Handlers;
using Brado.Protocol;
using Microsoft.Extensions.Logging;

namespace Brado.Broker.Server;

public enum ConnectionRole
{
    Unknown,
    Publisher,
    Subscriber,
    Both
}

internal sealed record SubscriptionBinding(string Topic, string Group, long SubscriptionId);

public sealed class ClientConnection
{
    private static long _ids;

    private readonly Socket _socket;
    private readonly NetworkStream _stream;
    private readonly IMessageBroker _broker;
    private readonly FrameHandlerRegistry _handlers;
    private readonly ConnectionRegistry _registry;
    private readonly BrokerOptions _options;
    private readonly ILogger _logger;
    private readonly Channel<Frame> _outbound;
    private readonly Dictionary<long, SubscriptionBinding> _subscriptions = new();
    private readonly object _subscriptionsGate = new();

    private long _framesIn, _framesOut, _published, _delivered;
    private int _role;

    internal ClientConnection(Socket socket, IMessageBroker broker, FrameHandlerRegistry handlers,
        ConnectionRegistry registry, BrokerOptions options, ILogger logger)
    {
        _socket = socket;
        _stream = new NetworkStream(socket, ownsSocket: true);
        _broker = broker;
        _handlers = handlers;
        _registry = registry;
        _options = options;
        _logger = logger;
        _outbound = Channel.CreateBounded<Frame>(new BoundedChannelOptions(options.OutboundQueueCapacity)
        {
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait
        });
        RemoteEndPoint = socket.RemoteEndPoint?.ToString() ?? "desconhecido";
    }

    public long Id { get; } = Interlocked.Increment(ref _ids);
    public string ClientName { get; private set; } = "anônimo";
    public string RemoteEndPoint { get; }
    public DateTimeOffset ConnectedAt { get; } = DateTimeOffset.UtcNow;
    public DateTimeOffset LastSeen { get; private set; } = DateTimeOffset.UtcNow;
    public ConnectionRole Role => (ConnectionRole)Volatile.Read(ref _role);
    public long FramesIn => Interlocked.Read(ref _framesIn);
    public long FramesOut => Interlocked.Read(ref _framesOut);
    public long Published => Interlocked.Read(ref _published);
    public long Delivered => Interlocked.Read(ref _delivered);
    public int QueuedFrames => _outbound.Reader.Count;

    public int SubscriptionCount
    {
        get { lock (_subscriptionsGate) return _subscriptions.Count; }
    }

    internal void SetClientName(string name) =>
        ClientName = string.IsNullOrWhiteSpace(name) ? "anônimo" : new string(name.Trim().Take(64).ToArray());

    internal void CountPublished()
    {
        Interlocked.Increment(ref _published);
        MarkRole(ConnectionRole.Publisher);
    }

    internal void CountDelivered() => Interlocked.Increment(ref _delivered);

    internal void AddSubscription(SubscriptionBinding binding)
    {
        lock (_subscriptionsGate) _subscriptions[binding.SubscriptionId] = binding;
        MarkRole(ConnectionRole.Subscriber);
    }

    internal SubscriptionBinding? GetSubscription(long subscriptionId)
    {
        lock (_subscriptionsGate) return _subscriptions.GetValueOrDefault(subscriptionId);
    }

    internal SubscriptionBinding? RemoveSubscription(long subscriptionId)
    {
        lock (_subscriptionsGate)
            return _subscriptions.Remove(subscriptionId, out var binding) ? binding : null;
    }

    internal ValueTask SendAsync(Frame frame, CancellationToken cancellationToken) =>
        _outbound.Writer.WriteAsync(frame, cancellationToken);

    internal bool TrySend(Frame frame) => _outbound.Writer.TryWrite(frame);

    internal async Task RunAsync(CancellationToken serverToken)
    {
        _registry.Add(this);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(serverToken);
        var writer = WriteLoopAsync(cts.Token);

        try
        {
            var reader = new FrameReader(_stream, _options.MaxPayloadBytes);
            while (!cts.IsCancellationRequested)
            {
                var frame = await reader.ReadAsync(cts.Token);
                if (frame is null) break;

                Interlocked.Increment(ref _framesIn);
                LastSeen = DateTimeOffset.UtcNow;
                await _handlers.HandleAsync(this, frame, cts.Token);
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (ProtocolException ex)
        {
            _logger.LogWarning("Conexão {Id} ({Remote}) violou o protocolo: {Message}", Id, RemoteEndPoint, ex.Message);
            TrySend(Frame.Error(0, ex.Message));
        }
        catch (Exception ex) when (ex is IOException or SocketException or ObjectDisposedException)
        {
            _logger.LogDebug("Conexão {Id} encerrada: {Message}", Id, ex.Message);
        }
        finally
        {
            _outbound.Writer.TryComplete();
            try { await writer.WaitAsync(TimeSpan.FromSeconds(2)); } catch { }
            await cts.CancelAsync();
            ReleaseSubscriptions();
            _registry.Remove(this);
            await _stream.DisposeAsync();
            _logger.LogInformation("Conexão {Id} ({Name}) fechada", Id, ClientName);
        }
    }

    private async Task WriteLoopAsync(CancellationToken cancellationToken)
    {
        var buffered = new BufferedStream(_stream, 64 * 1024);
        var reader = _outbound.Reader;
        try
        {
            while (await reader.WaitToReadAsync(cancellationToken))
            {
                while (reader.TryRead(out var frame))
                {
                    await FrameWriter.WriteAsync(buffered, frame, cancellationToken);
                    Interlocked.Increment(ref _framesOut);
                }
                await buffered.FlushAsync(cancellationToken);
            }
        }
        catch (Exception ex) when (ex is OperationCanceledException or IOException or SocketException or ObjectDisposedException)
        {
        }
    }

    private void ReleaseSubscriptions()
    {
        SubscriptionBinding[] bindings;
        lock (_subscriptionsGate)
        {
            bindings = _subscriptions.Values.ToArray();
            _subscriptions.Clear();
        }
        foreach (var binding in bindings)
            _broker.Unsubscribe(binding.Topic, binding.Group, binding.SubscriptionId);
    }

    private void MarkRole(ConnectionRole role)
    {
        while (true)
        {
            var current = Volatile.Read(ref _role);
            var next = current == (int)ConnectionRole.Unknown || current == (int)role ? (int)role : (int)ConnectionRole.Both;
            if (next == current || Interlocked.CompareExchange(ref _role, next, current) == current) return;
        }
    }

    internal void Abort()
    {
        try { _socket.Shutdown(SocketShutdown.Both); } catch { }
    }
}

internal sealed class ConnectionSubscription(ClientConnection connection, long subscriptionId, int prefetch) : IDeliveryTarget
{
    public long SubscriptionId { get; } = subscriptionId;
    public int Prefetch { get; } = prefetch;
    public string Description => $"tcp:{connection.Id}:{connection.ClientName}";

    public bool TryDeliver(string topic, string group, StoredMessage message, int attempt)
    {
        var frame = new Frame(FrameType.Deliver, SubscriptionId, topic, message.Payload, message.Offset,
            attempt > 1 ? FrameFlags.Redelivered : FrameFlags.None);
        if (!connection.TrySend(frame)) return false;
        connection.CountDelivered();
        return true;
    }
}
