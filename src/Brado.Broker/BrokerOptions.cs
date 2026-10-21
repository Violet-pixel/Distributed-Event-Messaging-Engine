namespace Brado.Broker;

public sealed class BrokerOptions
{
    public const string Section = "Broker";

    public string Host { get; set; } = "0.0.0.0";
    public int Port { get; set; } = 7420;
    public int MaxConnections { get; set; } = 2048;
    public int MaxPayloadBytes { get; set; } = 4 * 1024 * 1024;

    public int OutboundQueueCapacity { get; set; } = 8192;

    public int RetainedMessagesPerTopic { get; set; } = 100_000;

    public int DefaultPrefetch { get; set; } = 64;
    public TimeSpan AckTimeout { get; set; } = TimeSpan.FromSeconds(30);
    public int MaxDeliveryAttempts { get; set; } = 5;
    public string DeadLetterSuffix { get; set; } = ".dlq";

    public bool PersistMessages { get; set; } = true;
}
