using System.Threading.Channels;

namespace Brado.EventStore;

public sealed class EventAppendedSignal
{
    private readonly object _gate = new();
    private Channel<bool>[] _subscribers = [];

    public SignalSubscription Subscribe()
    {
        var channel = Channel.CreateBounded<bool>(new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.DropWrite });
        lock (_gate) _subscribers = [.. _subscribers, channel];
        return new SignalSubscription(channel.Reader);
    }

    public void Notify()
    {
        foreach (var channel in Volatile.Read(ref _subscribers))
            channel.Writer.TryWrite(true);
    }
}

public sealed class SignalSubscription(ChannelReader<bool> reader)
{
    public async Task WaitAsync(TimeSpan timeout, CancellationToken cancellationToken)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(timeout);
        try
        {
            await reader.ReadAsync(cts.Token);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
        }
    }
}
