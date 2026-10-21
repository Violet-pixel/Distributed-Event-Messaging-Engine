using System.Collections.Concurrent;

namespace Brado.Broker.Server;

public sealed record ConnectionSnapshot(
    long Id,
    string ClientName,
    string RemoteEndPoint,
    string Role,
    DateTimeOffset ConnectedAt,
    DateTimeOffset LastSeen,
    long Published,
    long Delivered,
    int Subscriptions,
    int QueuedFrames);

public sealed class ConnectionRegistry
{
    private readonly ConcurrentDictionary<long, ClientConnection> _connections = new();

    public int Count => _connections.Count;

    internal void Add(ClientConnection connection) => _connections[connection.Id] = connection;

    internal void Remove(ClientConnection connection) => _connections.TryRemove(connection.Id, out _);

    public IReadOnlyList<ConnectionSnapshot> Snapshot() =>
        _connections.Values
            .OrderBy(c => c.Id)
            .Select(c => new ConnectionSnapshot(c.Id, c.ClientName, c.RemoteEndPoint, c.Role.ToString(), c.ConnectedAt,
                c.LastSeen, c.Published, c.Delivered, c.SubscriptionCount, c.QueuedFrames))
            .ToArray();

    internal void AbortAll()
    {
        foreach (var connection in _connections.Values) connection.Abort();
    }
}
