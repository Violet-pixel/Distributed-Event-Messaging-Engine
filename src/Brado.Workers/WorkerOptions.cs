namespace Brado.Workers;

public enum BrokerMode
{
    InProcess,

    Tcp
}

public sealed class OutboxOptions
{
    public const string Section = "Outbox";

    public int BatchSize { get; set; } = 200;
    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromMilliseconds(500);
    public TimeSpan Lease { get; set; } = TimeSpan.FromSeconds(30);
    public TimeSpan BaseRetryDelay { get; set; } = TimeSpan.FromSeconds(1);
    public TimeSpan MaxRetryDelay { get; set; } = TimeSpan.FromMinutes(5);
    public TimeSpan DispatchedRetention { get; set; } = TimeSpan.FromDays(7);
}

public sealed class MessagingOptions
{
    public const string Section = "Messaging";

    public BrokerMode Mode { get; set; } = BrokerMode.InProcess;
    public string BrokerHost { get; set; } = "localhost";
    public int BrokerPort { get; set; } = 7420;
    public string ClientName { get; set; } = "brado-workers";
}

public sealed class ProjectionOptions
{
    public const string Section = "Projections";

    public int BatchSize { get; set; } = 500;
    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromSeconds(1);
}

public sealed class SagaOptions
{
    public const string Section = "Saga";

    public bool Enabled { get; set; } = true;
    public string Topic { get; set; } = "orders.events";
    public double PaymentFailureRate { get; set; } = 0.08;
    public double StockFailureRate { get; set; } = 0.10;
    public int SimulatedLatencyMs { get; set; } = 120;
}
