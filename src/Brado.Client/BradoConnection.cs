using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Brado.Protocol;

namespace Brado.Client;

public sealed class BradoClientOptions
{
    public string Host { get; set; } = "localhost";
    public int Port { get; set; } = 7420;
    public string ClientName { get; set; } = "brado-client";
    public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(10);
    public TimeSpan KeepAliveInterval { get; set; } = TimeSpan.FromSeconds(15);
    public int MaxPayloadBytes { get; set; } = FrameCodec.DefaultMaxPayloadBytes;
}

public sealed record ReceivedMessage(string Topic, string Group, long Offset, ReadOnlyMemory<byte> Payload, bool Redelivered)
{
    public string AsString() => Encoding.UTF8.GetString(Payload.Span);

    public T? AsJson<T>(JsonSerializerOptions? options = null) =>
        JsonSerializer.Deserialize<T>(Payload.Span, options ?? new JsonSerializerOptions(JsonSerializerDefaults.Web));
}

public sealed class BradoException(string message) : Exception(message);

public sealed class BradoConnection : IAsyncDisposable
{
    private readonly TcpClient _tcp;
    private readonly NetworkStream _stream;
    private readonly BradoClientOptions _options;
    private readonly Channel<Frame> _outbound = Channel.CreateUnbounded<Frame>(new UnboundedChannelOptions { SingleReader = true });
    private readonly ConcurrentDictionary<long, TaskCompletionSource<Frame>> _pending = new();
    private readonly ConcurrentDictionary<long, ClientSubscription> _pendingSubscriptions = new();
    private readonly ConcurrentDictionary<long, ClientSubscription> _subscriptions = new();
    private readonly CancellationTokenSource _cts = new();
    private Task _readLoop = Task.CompletedTask;
    private Task _writeLoop = Task.CompletedTask;
    private Task _keepAlive = Task.CompletedTask;
    private long _correlation;

    private BradoConnection(TcpClient tcp, BradoClientOptions options)
    {
        _tcp = tcp;
        _stream = tcp.GetStream();
        _options = options;
    }

    public long ConnectionId { get; private set; }
    public bool IsConnected => _tcp.Connected && !_cts.IsCancellationRequested;

    public static async Task<BradoConnection> ConnectAsync(BradoClientOptions options, CancellationToken cancellationToken = default)
    {
        var tcp = new TcpClient { NoDelay = true };
        await tcp.ConnectAsync(options.Host, options.Port, cancellationToken);

        var connection = new BradoConnection(tcp, options);
        connection.Start();

        var ack = await connection.RequestAsync(new Frame(FrameType.Hello, Payload: Encoding.UTF8.GetBytes(options.ClientName)), cancellationToken);
        connection.ConnectionId = PayloadCodec.ReadInt64(ack.Payload.Span);
        return connection;
    }

    public static Task<BradoConnection> ConnectAsync(string host = "localhost", int port = 7420, string clientName = "brado-client",
        CancellationToken cancellationToken = default) =>
        ConnectAsync(new BradoClientOptions { Host = host, Port = port, ClientName = clientName }, cancellationToken);

    public async Task<long> PublishAsync(string topic, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken = default)
    {
        TopicName.EnsureValid(topic);
        var ack = await RequestAsync(new Frame(FrameType.Publish, Topic: topic, Payload: payload), cancellationToken);
        return ack.Offset;
    }

    public Task<long> PublishAsync(string topic, string text, CancellationToken cancellationToken = default) =>
        PublishAsync(topic, Encoding.UTF8.GetBytes(text), cancellationToken);

    public Task<long> PublishJsonAsync<T>(string topic, T message, JsonSerializerOptions? options = null, CancellationToken cancellationToken = default) =>
        PublishAsync(topic, JsonSerializer.SerializeToUtf8Bytes(message, options ?? new JsonSerializerOptions(JsonSerializerDefaults.Web)), cancellationToken);

    public async Task<IAsyncDisposable> SubscribeAsync(
        string topic,
        string group,
        Func<ReceivedMessage, CancellationToken, Task> handler,
        int prefetch = 64,
        long startOffset = StartPosition.Latest,
        CancellationToken cancellationToken = default)
    {
        TopicName.EnsureValid(topic);
        TopicName.EnsureValid(group);

        var correlationId = Interlocked.Increment(ref _correlation);
        var subscription = new ClientSubscription(this, topic, group, handler);
        _pendingSubscriptions[correlationId] = subscription;

        try
        {
            var request = new SubscribeRequest(group, startOffset, prefetch).ToBytes();
            await RequestAsync(new Frame(FrameType.Subscribe, correlationId, topic, request), cancellationToken, correlationId);
        }
        finally
        {
            _pendingSubscriptions.TryRemove(correlationId, out _);
        }

        subscription.Start(_cts.Token);
        return subscription;
    }

    public async Task<TimeSpan> PingAsync(CancellationToken cancellationToken = default)
    {
        var started = System.Diagnostics.Stopwatch.GetTimestamp();
        await RequestAsync(new Frame(FrameType.Ping), cancellationToken);
        return System.Diagnostics.Stopwatch.GetElapsedTime(started);
    }

    internal void Ack(long subscriptionId, long offset) =>
        _outbound.Writer.TryWrite(new Frame(FrameType.Ack, subscriptionId, Offset: offset));

    internal void Nack(long subscriptionId, long offset, bool requeue) =>
        _outbound.Writer.TryWrite(new Frame(FrameType.Nack, subscriptionId, Offset: offset, Flags: requeue ? FrameFlags.Requeue : FrameFlags.None));

    internal void Unsubscribe(ClientSubscription subscription)
    {
        if (_subscriptions.TryRemove(subscription.SubscriptionId, out _))
            _outbound.Writer.TryWrite(new Frame(FrameType.Unsubscribe, subscription.SubscriptionId, subscription.Topic));
    }

    private void Start()
    {
        _readLoop = Task.Run(() => ReadLoopAsync(_cts.Token));
        _writeLoop = Task.Run(() => WriteLoopAsync(_cts.Token));
        _keepAlive = Task.Run(() => KeepAliveAsync(_cts.Token));
    }

    private async Task<Frame> RequestAsync(Frame frame, CancellationToken cancellationToken, long? correlationId = null)
    {
        var id = correlationId ?? Interlocked.Increment(ref _correlation);
        var tcs = new TaskCompletionSource<Frame>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pending[id] = tcs;
        try
        {
            if (!_outbound.Writer.TryWrite(frame with { CorrelationId = id }))
                throw new BradoException("Conexão encerrada.");

            var response = await tcs.Task.WaitAsync(_options.RequestTimeout, cancellationToken);
            if (response.Type == FrameType.Error)
                throw new BradoException(PayloadCodec.Utf8(response.Payload));
            return response;
        }
        finally
        {
            _pending.TryRemove(id, out _);
        }
    }

    private async Task ReadLoopAsync(CancellationToken cancellationToken)
    {
        var reader = new FrameReader(_stream, _options.MaxPayloadBytes);
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var frame = await reader.ReadAsync(cancellationToken);
                if (frame is null) break;

                switch (frame.Type)
                {
                    case FrameType.Deliver:
                        if (_subscriptions.TryGetValue(frame.CorrelationId, out var target))
                            target.Enqueue(frame);
                        break;

                    case FrameType.SubscribeAck:

                        if (_pendingSubscriptions.TryGetValue(frame.CorrelationId, out var pending))
                        {
                            pending.SubscriptionId = PayloadCodec.ReadInt64(frame.Payload.Span);
                            _subscriptions[pending.SubscriptionId] = pending;
                        }
                        Complete(frame);
                        break;

                    default:
                        Complete(frame);
                        break;
                }
            }
        }
        catch (Exception ex) when (ex is OperationCanceledException or IOException or SocketException or ObjectDisposedException)
        {
        }
        finally
        {
            FailPending(new BradoException("Conexão com o broker encerrada."));
        }
    }

    private void Complete(Frame frame)
    {
        if (_pending.TryGetValue(frame.CorrelationId, out var tcs))
            tcs.TrySetResult(frame);
    }

    private void FailPending(Exception error)
    {
        foreach (var tcs in _pending.Values) tcs.TrySetException(error);
    }

    private async Task WriteLoopAsync(CancellationToken cancellationToken)
    {
        var buffered = new BufferedStream(_stream, 64 * 1024);
        try
        {
            while (await _outbound.Reader.WaitToReadAsync(cancellationToken))
            {
                while (_outbound.Reader.TryRead(out var frame))
                    await FrameWriter.WriteAsync(buffered, frame, cancellationToken);
                await buffered.FlushAsync(cancellationToken);
            }
        }
        catch (Exception ex) when (ex is OperationCanceledException or IOException or SocketException or ObjectDisposedException)
        {
        }
    }

    private async Task KeepAliveAsync(CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(_options.KeepAliveInterval);
        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken))
            {
                try { await PingAsync(cancellationToken); }
                catch (BradoException) { }
                catch (TimeoutException) { }
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var subscription in _subscriptions.Values.ToArray())
            await subscription.DisposeAsync();

        _outbound.Writer.TryComplete();
        try { await _writeLoop.WaitAsync(TimeSpan.FromSeconds(2)); } catch { }
        await _cts.CancelAsync();
        try { await Task.WhenAll(_readLoop, _keepAlive).WaitAsync(TimeSpan.FromSeconds(2)); } catch { }
        _tcp.Dispose();
        _cts.Dispose();
    }
}

internal sealed class ClientSubscription(
    BradoConnection connection,
    string topic,
    string group,
    Func<ReceivedMessage, CancellationToken, Task> handler) : IAsyncDisposable
{
    private readonly Channel<Frame> _inbox = Channel.CreateUnbounded<Frame>(new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });
    private Task _loop = Task.CompletedTask;
    private int _disposed;

    public long SubscriptionId { get; set; }
    public string Topic { get; } = topic;
    public string Group { get; } = group;

    public void Enqueue(Frame frame) => _inbox.Writer.TryWrite(frame);

    public void Start(CancellationToken cancellationToken) => _loop = Task.Run(() => ConsumeAsync(cancellationToken));

    private async Task ConsumeAsync(CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var frame in _inbox.Reader.ReadAllAsync(cancellationToken))
            {
                var message = new ReceivedMessage(frame.Topic, Group, frame.Offset, frame.Payload, frame.Flags.HasFlag(FrameFlags.Redelivered));
                try
                {
                    await handler(message, cancellationToken);
                    connection.Ack(SubscriptionId, frame.Offset);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch
                {
                    connection.Nack(SubscriptionId, frame.Offset, requeue: true);
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1) return;
        connection.Unsubscribe(this);
        _inbox.Writer.TryComplete();
        try { await _loop.WaitAsync(TimeSpan.FromSeconds(5)); } catch { }
    }
}
