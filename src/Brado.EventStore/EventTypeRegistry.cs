using System.Reflection;
using System.Text.Json;
using Brado.Domain;
using Brado.Domain.Contracts;

namespace Brado.EventStore;

public sealed class EventTypeRegistry
{
    private readonly Dictionary<string, Type> _byName = new(StringComparer.Ordinal);
    private readonly Dictionary<Type, string> _byType = new();

    public EventTypeRegistry RegisterAssembly(Assembly assembly)
    {
        foreach (var type in assembly.GetTypes().Where(t => t is { IsAbstract: false, IsInterface: false } && typeof(IDomainEvent).IsAssignableFrom(t)))
            Register(type);
        return this;
    }

    public EventTypeRegistry Register(Type type, string? name = null)
    {
        name ??= type.Name;
        _byName[name] = type;
        _byType[type] = name;
        return this;
    }

    public IReadOnlyCollection<string> Names => _byName.Keys;

    public string GetName(Type type) =>
        _byType.TryGetValue(type, out var name) ? name : throw new InvalidOperationException($"Evento {type.Name} não registrado.");

    public Type? Resolve(string name) => _byName.GetValueOrDefault(name);

    public string Serialize(IDomainEvent @event) => JsonSerializer.Serialize(@event, @event.GetType(), Json.Options);

    public IDomainEvent Deserialize(string eventType, string payload)
    {
        var type = Resolve(eventType) ?? throw new InvalidOperationException($"Tipo de evento desconhecido: {eventType}.");
        return (IDomainEvent)(JsonSerializer.Deserialize(payload, type, Json.Options)
            ?? throw new InvalidOperationException($"Payload vazio para {eventType}."));
    }

    public bool TryDeserialize(string eventType, string payload, out IDomainEvent? @event)
    {
        var type = Resolve(eventType);
        @event = type is null ? null : JsonSerializer.Deserialize(payload, type, Json.Options) as IDomainEvent;
        return @event is not null;
    }
}
