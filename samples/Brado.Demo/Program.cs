using System.Diagnostics;
using Brado.Client;
using Brado.Protocol;

var command = args.FirstOrDefault() ?? "run";
var options = Options.Parse(args.Skip(1).ToArray());
using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

Console.WriteLine($"Brado demo · {command} · tcp://{options.Host}:{options.Port}");

try
{
    switch (command)
    {
        case "publish":
            await RunPublishersAsync(options, cts.Token);
            break;
        case "subscribe":
            await RunSubscribersAsync(options, cts.Token);
            break;
        case "bench":
            await RunBenchmarkAsync(options, cts.Token);
            break;
        default:
            await Task.WhenAll(RunSubscribersAsync(options, cts.Token), RunPublishersAsync(options, cts.Token));
            break;
    }
}
catch (OperationCanceledException)
{
}

Console.WriteLine("Encerrado.");

static async Task RunPublishersAsync(Options o, CancellationToken ct)
{
    var tasks = Enumerable.Range(1, o.Publishers).Select(async index =>
    {
        await using var connection = await BradoConnection.ConnectAsync(o.Host, o.Port, $"demo-publisher-{index}", ct);
        var perPublisher = Math.Max(1, o.Rate / o.Publishers);
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(100));
        long sent = 0;
        while (await timer.WaitForNextTickAsync(ct))
        {
            var batch = Math.Max(1, perPublisher / 10);
            var publishes = Enumerable.Range(0, batch).Select(_ => connection.PublishJsonAsync(o.Topic, new
            {
                sensor = $"sensor-{Random.Shared.Next(1, 50)}",
                temperatura = Math.Round(18 + Random.Shared.NextDouble() * 12, 2),
                publisher = index,
                em = DateTimeOffset.UtcNow
            }, cancellationToken: ct));
            await Task.WhenAll(publishes);
            sent += batch;
            if (sent % (perPublisher * 5) < batch) Console.WriteLine($"publisher {index}: {sent:N0} enviadas");
        }
    });
    await Task.WhenAll(tasks);
}

static async Task RunSubscribersAsync(Options o, CancellationToken ct)
{
    var connections = new List<BradoConnection>();
    long received = 0;
    for (var i = 1; i <= o.Consumers; i++)
    {
        var connection = await BradoConnection.ConnectAsync(o.Host, o.Port, $"demo-consumer-{i}", ct);
        connections.Add(connection);
        await connection.SubscribeAsync(o.Topic, o.Group, async (message, token) =>
        {
            Interlocked.Increment(ref received);
            if (o.WorkMs > 0) await Task.Delay(Random.Shared.Next(0, o.WorkMs + 1), token);
        }, prefetch: 128, startOffset: StartPosition.Latest, cancellationToken: ct);
    }

    using var timer = new PeriodicTimer(TimeSpan.FromSeconds(2));
    try
    {
        while (await timer.WaitForNextTickAsync(ct))
            Console.WriteLine($"grupo {o.Group}: {Interlocked.Read(ref received):N0} recebidas");
    }
    finally
    {
        foreach (var connection in connections) await connection.DisposeAsync();
    }
}

static async Task RunBenchmarkAsync(Options o, CancellationToken ct)
{
    await using var publisher = await BradoConnection.ConnectAsync(o.Host, o.Port, "bench-publisher", ct);
    var payload = new byte[o.Size];
    Random.Shared.NextBytes(payload);

    var watch = Stopwatch.StartNew();
    const int window = 512;
    var inFlight = new List<Task<long>>(window);
    for (var i = 0; i < o.Messages; i++)
    {
        inFlight.Add(publisher.PublishAsync(o.Topic, payload, ct));
        if (inFlight.Count == window)
        {
            await Task.WhenAll(inFlight);
            inFlight.Clear();
        }
    }
    await Task.WhenAll(inFlight);
    watch.Stop();

    var rate = o.Messages / watch.Elapsed.TotalSeconds;
    Console.WriteLine($"{o.Messages:N0} mensagens de {o.Size} bytes em {watch.Elapsed.TotalSeconds:N2}s → {rate:N0} msg/s ({rate * o.Size / 1024 / 1024:N1} MiB/s)");
}

internal sealed record Options(string Host, int Port, string Topic, string Group, int Rate, int Publishers, int Consumers, int WorkMs, int Messages, int Size)
{
    public static Options Parse(string[] args)
    {
        string Get(string name, string fallback)
        {
            var index = Array.IndexOf(args, $"--{name}");
            return index >= 0 && index + 1 < args.Length ? args[index + 1] : fallback;
        }

        return new Options(
            Get("host", "localhost"),
            int.Parse(Get("port", "7420")),
            Get("topic", "demo.metricas"),
            Get("group", "painel"),
            int.Parse(Get("rate", "400")),
            int.Parse(Get("publishers", "3")),
            int.Parse(Get("consumers", "2")),
            int.Parse(Get("work-ms", "3")),
            int.Parse(Get("messages", "100000")),
            int.Parse(Get("size", "256")));
    }
}
