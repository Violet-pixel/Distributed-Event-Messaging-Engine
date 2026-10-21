using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using Brado.Broker.Metrics;
using Brado.Protocol;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Brado.Broker;

public interface IMessageBroker
{
    ValueTask<long> PublishAsync(string topic, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken = default);

    long NextSubscriptionId();

    ConsumerGroup Subscribe(string topic, string group, IDeliveryTarget target, long startOffset = StartPosition.Latest);

    void Unsubscribe(string topic, string group, long subscriptionId);

    bool Ack(string topic, string group, long subscriptionId, long offset);

    bool Nack(string topic, string group, long subscriptionId, long offset, bool requeue);

    IAsyncDisposable SubscribeLocal(string topic, string group, Func<Delivery, CancellationToken, ValueTask> handler,
        int prefetch = 32, long startOffset = StartPosition.Earliest);

    IReadOnlyCollection<Topic> Topics { get; }
}

public sealed class MessageBroker : IMessageBroker, IAsyncDisposable
{
    private readonly ConcurrentDictionary<string, Topic> _topics = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<(string Topic, string Group), long> _restoredOffsets = new();
    private readonly ITopicFactory _topicFactory;
    private readonly IMessageJournal _journal;
    private readonly BrokerMetrics _metrics;
    private readonly BrokerOptions _options;
    private readonly ILogger<MessageBroker> _logger;
    private readonly SemaphoreSlim _initGate = new(1, 1);
    private volatile bool _initialized;
    private long _subscriptionIds;

    public MessageBroker(
        ITopicFactory topicFactory,
        IMessageJournal journal,
        BrokerMetrics metrics,
        IOptions<BrokerOptions> options,
        ILogger<MessageBroker> logger)
    {
        _topicFactory = topicFactory;
        _journal = journal;
        _metrics = metrics;
        _options = options.Value;
        _logger = logger;
    }

    public IReadOnlyCollection<Topic> Topics => _topics.Values.ToArray();

    public BrokerMetrics Metrics => _metrics;

    public BrokerOptions Options => _options;

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        if (_initialized) return;
        await _initGate.WaitAsync(cancellationToken);
        try
        {
            if (_initialized) return;
            if (_options.PersistMessages) await RestoreFromJournalAsync(cancellationToken);
            _initialized = true;
        }
        finally
        {
            _initGate.Release();
        }
    }

    private async Task RestoreFromJournalAsync(CancellationToken cancellationToken)
    {
        foreach (var name in await _journal.ListTopicsAsync(cancellationToken))
        {
            var messages = await _journal.ReadTailAsync(name, _options.RetainedMessagesPerTopic, cancellationToken);
            GetOrCreateTopic(name).Restore(messages);
        }

        foreach (var offset in await _journal.LoadOffsetsAsync(cancellationToken))
            _restoredOffsets[(offset.Topic, offset.Group)] = offset.CommittedOffset;

        _logger.LogInformation("Journal restaurado: {Topics} tópicos, {Groups} grupos", _topics.Count, _restoredOffsets.Count);
    }

    public Topic GetOrCreateTopic(string name)
    {
        if (_topics.TryGetValue(name, out var topic)) return topic;
        TopicName.EnsureValid(name);
        return _topics.GetOrAdd(name, static (n, factory) => factory.Create(n), _topicFactory);
    }

    public bool TryGetTopic(string name, [MaybeNullWhen(false)] out Topic topic) => _topics.TryGetValue(name, out topic);

    public ValueTask<long> PublishAsync(string topic, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken = default) =>
        PublishOwnedAsync(topic, payload.ToArray(), cancellationToken);

    public async ValueTask<long> PublishOwnedAsync(string topic, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken = default)
    {
        if (payload.Length > _options.MaxPayloadBytes)
            throw new ProtocolException($"Payload acima de {_options.MaxPayloadBytes} bytes.");

        var target = GetOrCreateTopic(topic);
        var message = target.Append(payload);
        _metrics.Published(payload.Length);

        if (_options.PersistMessages)
            await _journal.AppendAsync(target.Name, message, cancellationToken).ConfigureAwait(false);

        return message.Offset;
    }

    public long NextSubscriptionId() => Interlocked.Increment(ref _subscriptionIds);

    public ConsumerGroup Subscribe(string topic, string group, IDeliveryTarget target, long startOffset = StartPosition.Latest)
    {
        TopicName.EnsureValid(group);
        var t = GetOrCreateTopic(topic);
        var consumerGroup = t.GetOrAddGroup(group, name =>
        {
            var start = _restoredOffsets.TryGetValue((t.Name, name), out var saved) ? saved : startOffset;
            return new ConsumerGroup(t, name, start, _options, _metrics, DeadLetterAsync, _logger);
        });
        consumerGroup.AddMember(target);
        return consumerGroup;
    }

    public void Unsubscribe(string topic, string group, long subscriptionId)
    {
        if (_topics.TryGetValue(topic, out var t) && t.TryGetGroup(group, out var g))
            g.RemoveMember(subscriptionId);
    }

    public bool Ack(string topic, string group, long subscriptionId, long offset) =>
        _topics.TryGetValue(topic, out var t) && t.TryGetGroup(group, out var g) && g.Ack(subscriptionId, offset);

    public bool Nack(string topic, string group, long subscriptionId, long offset, bool requeue) =>
        _topics.TryGetValue(topic, out var t) && t.TryGetGroup(group, out var g) && g.Nack(subscriptionId, offset, requeue);

    public IAsyncDisposable SubscribeLocal(string topic, string group, Func<Delivery, CancellationToken, ValueTask> handler,
        int prefetch = 32, long startOffset = StartPosition.Earliest)
    {
        var subscription = new LocalSubscription(this, topic, group, NextSubscriptionId(), prefetch, handler, _logger);
        Subscribe(topic, group, subscription, startOffset);
        subscription.Start();
        return subscription;
    }

    public async Task RunMaintenanceAsync(CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        var tick = 0;
        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken))
            {
                _metrics.Sample();
                var now = DateTimeOffset.UtcNow;
                foreach (var topic in _topics.Values)
                foreach (var group in topic.Groups)
                {
                    group.ExpireInFlight(now);
                    group.Nudge();
                }

                if (++tick % 5 == 0) await PersistOffsetsAsync(cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            await PersistOffsetsAsync(CancellationToken.None);
        }
    }

    public IReadOnlyList<GroupOffset> CollectOffsets() =>
        _topics.Values.SelectMany(t => t.Groups.Select(g => new GroupOffset(t.Name, g.Name, g.Committed))).ToArray();

    private async Task PersistOffsetsAsync(CancellationToken cancellationToken)
    {
        if (!_options.PersistMessages) return;
        try
        {
            var offsets = CollectOffsets();
            if (offsets.Count > 0) await _journal.SaveOffsetsAsync(offsets, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(ex, "Falha ao gravar offsets dos grupos");
        }
    }

    private async ValueTask DeadLetterAsync(string topic, StoredMessage message)
    {
        if (topic.EndsWith(_options.DeadLetterSuffix, StringComparison.Ordinal))
        {
            _logger.LogWarning("Mensagem {Offset} descartada: já estava na DLQ {Topic}", message.Offset, topic);
            return;
        }
        await PublishOwnedAsync(topic + _options.DeadLetterSuffix, message.Payload);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var topic in _topics.Values)
        foreach (var group in topic.Groups)
            await group.DisposeAsync();
    }
}
