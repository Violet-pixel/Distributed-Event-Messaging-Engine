using System.Text.Json;
using System.Text.Json.Serialization;

namespace Brado.Domain.Contracts;

public sealed record EventEnvelope(
    Guid MessageId,
    string EventType,
    string StreamId,
    long StreamVersion,
    DateTimeOffset OccurredAt,
    JsonElement Data)
{
    public T ReadData<T>() => Data.Deserialize<T>(Json.Options)
        ?? throw new InvalidOperationException($"Envelope {MessageId} sem dados de {typeof(T).Name}.");
}

public static class Json
{
    public static readonly JsonSerializerOptions Options = Create();

    private static JsonSerializerOptions Create()
    {
        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web);
        options.Converters.Add(new JsonStringEnumConverter());
        return options;
    }
}
