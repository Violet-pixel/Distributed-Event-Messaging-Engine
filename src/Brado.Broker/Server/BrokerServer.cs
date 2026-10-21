using System.Net;
using System.Net.Sockets;
using Brado.Broker.Handlers;
using Brado.Protocol;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Brado.Broker.Server;

public sealed class BrokerServer : BackgroundService
{
    private readonly MessageBroker _broker;
    private readonly FrameHandlerRegistry _handlers;
    private readonly ConnectionRegistry _registry;
    private readonly BrokerOptions _options;
    private readonly ILogger<BrokerServer> _logger;
    private readonly TaskCompletionSource<IPEndPoint> _started = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private TcpListener? _listener;

    public BrokerServer(MessageBroker broker, FrameHandlerRegistry handlers, ConnectionRegistry registry,
        IOptions<BrokerOptions> options, ILogger<BrokerServer> logger)
    {
        _broker = broker;
        _handlers = handlers;
        _registry = registry;
        _options = options.Value;
        _logger = logger;
    }

    public Task<IPEndPoint> Started => _started.Task;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await _broker.InitializeAsync(stoppingToken);

        _listener = new TcpListener(IPAddress.Parse(_options.Host), _options.Port);
        _listener.Start(backlog: 1024);
        var endpoint = (IPEndPoint)_listener.LocalEndpoint;
        _started.TrySetResult(endpoint);
        _logger.LogInformation("Broker Brado escutando em tcp://{Endpoint}", endpoint);

        var maintenance = _broker.RunMaintenanceAsync(stoppingToken);
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var socket = await _listener.AcceptSocketAsync(stoppingToken);
                if (_registry.Count >= _options.MaxConnections)
                {
                    _logger.LogWarning("Limite de {Max} conexões atingido; recusando {Remote}", _options.MaxConnections, socket.RemoteEndPoint);
                    await RejectAsync(socket);
                    continue;
                }

                socket.NoDelay = true;
                var connection = new ClientConnection(socket, _broker, _handlers, _registry, _options, _logger);
                _ = Task.Run(() => connection.RunAsync(stoppingToken), CancellationToken.None);
            }
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            _listener.Stop();
            _registry.AbortAll();
            await maintenance;
        }
    }

    private static async Task RejectAsync(Socket socket)
    {
        try
        {
            await using var stream = new NetworkStream(socket, ownsSocket: true);
            await FrameWriter.WriteAsync(stream, Frame.Error(0, "Servidor lotado, tente novamente."));
            await stream.FlushAsync();
        }
        catch
        {
        }
    }
}
