using Brado.Broker;
using Brado.Client;
using Brado.EventStore.Outbox;
using Brado.Protocol;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Brado.Workers.Messaging;

public delegate Task MessageHandler(ReadOnlyMemory<byte> payload, bool redelivered, CancellationToken cancellationToken);

public interface IMessageSubscriber
{
    Task<IAsyncDisposable> SubscribeAsync(string topic, string group, MessageHandler handler, int prefetch, CancellationToken cancellationToken);
}

public sealed class InProcessMessagePublisher(MessageBroker broker) : IMessagePublisher
{
    public async Task PublishAsync(string topic, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken = default) =>
        await broker.PublishOwnedAsync(topic, payload, cancellationToken);
}

public sealed class InProcessMessageSubscriber(MessageBroker broker) : IMessageSubscriber
{
    public async Task<IAsyncDisposable> SubscribeAsync(string topic, string group, MessageHandler handler, int prefetch, CancellationToken cancellationToken)
    {
        await broker.InitializeAsync(cancellationToken);
        return broker.SubscribeLocal(topic, group, (d, ct) => new ValueTask(handler(d.Payload, d.Redelivered, ct)), prefetch, StartPosition.Earliest);
    }
}

public sealed class SharedBrokerConnection(IOptions<MessagingOptions> options, ILogger<SharedBrokerConnection> logger) : IAsyncDisposable
{
    private readonly SemaphoreSlim _gate = new(1, 1);
    private BradoConnection? _connection;

    public async Task<BradoConnection> GetAsync(CancellationToken cancellationToken)
    {
        if (_connection is { IsConnected: true } current) return current;

        await _gate.WaitAsync(cancellationToken);
        try
        {
            if (_connection is { IsConnected: true } existing) return existing;
            if (_connection is not null) await _connection.DisposeAsync();

            var o = options.Value;
            _connection = await BradoConnection.ConnectAsync(new BradoClientOptions { Host = o.BrokerHost, Port = o.BrokerPort, ClientName = o.ClientName }, cancellationToken);
            logger.LogInformation("Conectado ao broker remoto {Host}:{Port}", o.BrokerHost, o.BrokerPort);
            return _connection;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task ResetAsync()
    {
        await _gate.WaitAsync();
        try
        {
            if (_connection is not null) await _connection.DisposeAsync();
            _connection = null;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_connection is not null) await _connection.DisposeAsync();
    }
}

public sealed class TcpMessagePublisher(SharedBrokerConnection connection) : IMessagePublisher
{
    public async Task PublishAsync(string topic, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken = default)
    {
        var client = await connection.GetAsync(cancellationToken);
        try
        {
            await client.PublishAsync(topic, payload, cancellationToken);
        }
        catch (Exception ex) when (ex is BradoException or IOException or TimeoutException or System.Net.Sockets.SocketException)
        {
            await connection.ResetAsync();
            throw;
        }
    }
}

public sealed class TcpMessageSubscriber(SharedBrokerConnection connection) : IMessageSubscriber
{
    public async Task<IAsyncDisposable> SubscribeAsync(string topic, string group, MessageHandler handler, int prefetch, CancellationToken cancellationToken)
    {
        var client = await connection.GetAsync(cancellationToken);
        return await client.SubscribeAsync(topic, group, (m, ct) => handler(m.Payload, m.Redelivered, ct), prefetch, StartPosition.Earliest, cancellationToken);
    }
}
