using System.Text;
using Brado.Protocol;

namespace Brado.UnitTests;

public sealed class BrokerTests
{
    private static byte[] Msg(int i) => Encoding.UTF8.GetBytes($"m{i}");

    [Fact]
    public async Task Offsets_are_monotonic_per_topic()
    {
        await using var broker = TestBroker.Create();
        var offsets = new List<long>();
        for (var i = 0; i < 5; i++) offsets.Add(await broker.PublishAsync("t", Msg(i)));
        Assert.Equal(new long[] { 0, 1, 2, 3, 4 }, offsets);
    }

    [Fact]
    public async Task Members_of_the_same_group_compete_for_messages()
    {
        await using var broker = TestBroker.Create();
        var a = new FakeTarget(broker.NextSubscriptionId(), 100);
        var b = new FakeTarget(broker.NextSubscriptionId(), 100);
        broker.Subscribe("pedidos", "workers", a, StartPosition.Earliest);
        broker.Subscribe("pedidos", "workers", b, StartPosition.Earliest);

        for (var i = 0; i < 20; i++) await broker.PublishAsync("pedidos", Msg(i));

        await Eventually.TrueAsync(() => a.Received.Count + b.Received.Count == 20);
        Assert.True(a.Received.Count > 0 && b.Received.Count > 0);
        var all = a.Received.Concat(b.Received).Select(r => r.Message.Offset).Order();
        Assert.Equal(Enumerable.Range(0, 20).Select(i => (long)i), all);
    }

    [Fact]
    public async Task Each_group_receives_every_message()
    {
        await using var broker = TestBroker.Create();
        var analytics = new FakeTarget(broker.NextSubscriptionId(), 100);
        var audit = new FakeTarget(broker.NextSubscriptionId(), 100);
        broker.Subscribe("eventos", "analytics", analytics, StartPosition.Earliest);
        broker.Subscribe("eventos", "auditoria", audit, StartPosition.Earliest);

        for (var i = 0; i < 10; i++) await broker.PublishAsync("eventos", Msg(i));

        await Eventually.TrueAsync(() => analytics.Received.Count == 10 && audit.Received.Count == 10);
    }

    [Fact]
    public async Task Prefetch_limits_unacknowledged_deliveries()
    {
        await using var broker = TestBroker.Create();
        var target = new FakeTarget(broker.NextSubscriptionId(), prefetch: 2);
        var group = broker.Subscribe("t", "g", target, StartPosition.Earliest);

        for (var i = 0; i < 10; i++) await broker.PublishAsync("t", Msg(i));
        await Eventually.TrueAsync(() => target.Received.Count == 2);
        await Task.Delay(100);
        Assert.Equal(2, target.Received.Count);

        Assert.True(group.Ack(target.SubscriptionId, 0));
        await Eventually.TrueAsync(() => target.Received.Count == 3);
    }

    [Fact]
    public async Task Committed_offset_advances_only_over_contiguous_acks()
    {
        await using var broker = TestBroker.Create();
        var target = new FakeTarget(broker.NextSubscriptionId(), 10);
        var group = broker.Subscribe("t", "g", target, StartPosition.Earliest);
        for (var i = 0; i < 3; i++) await broker.PublishAsync("t", Msg(i));
        await Eventually.TrueAsync(() => target.Received.Count == 3);

        group.Ack(target.SubscriptionId, 1);
        Assert.Equal(0, group.Committed);
        group.Ack(target.SubscriptionId, 0);
        Assert.Equal(2, group.Committed);
        group.Ack(target.SubscriptionId, 2);
        Assert.Equal(3, group.Committed);
        Assert.Equal(0, group.Snapshot().Lag);
    }

    [Fact]
    public async Task Nack_with_requeue_redelivers_with_next_attempt()
    {
        await using var broker = TestBroker.Create();
        var target = new FakeTarget(broker.NextSubscriptionId());
        var group = broker.Subscribe("t", "g", target, StartPosition.Earliest);
        await broker.PublishAsync("t", Msg(0));
        await Eventually.TrueAsync(() => target.Received.Count == 1);

        group.Nack(target.SubscriptionId, 0, requeue: true);

        await Eventually.TrueAsync(() => target.Received.Count == 2);
        Assert.Equal(2, target.Received.Last().Attempt);
    }

    [Fact]
    public async Task Message_goes_to_dead_letter_topic_after_max_attempts()
    {
        await using var broker = TestBroker.Create(o => o.MaxDeliveryAttempts = 2);
        var target = new FakeTarget(broker.NextSubscriptionId());
        var group = broker.Subscribe("pagamentos", "g", target, StartPosition.Earliest);
        await broker.PublishAsync("pagamentos", Msg(7));

        for (var attempt = 1; attempt <= 2; attempt++)
        {
            await Eventually.TrueAsync(() => target.Received.Count == attempt);
            group.Nack(target.SubscriptionId, 0, requeue: true);
        }

        await Eventually.TrueAsync(() => broker.TryGetTopic("pagamentos.dlq", out var dlq) && dlq.NextOffset == 1);
        Assert.Equal(0, group.Snapshot().InFlight);
    }

    [Fact]
    public async Task Removing_a_member_returns_its_messages_to_the_group()
    {
        await using var broker = TestBroker.Create();
        var leaving = new FakeTarget(broker.NextSubscriptionId(), 5);
        var group = broker.Subscribe("t", "g", leaving, StartPosition.Earliest);
        for (var i = 0; i < 3; i++) await broker.PublishAsync("t", Msg(i));
        await Eventually.TrueAsync(() => leaving.Received.Count == 3);

        var staying = new FakeTarget(broker.NextSubscriptionId(), 5);
        group.AddMember(staying);
        group.RemoveMember(leaving.SubscriptionId);

        await Eventually.TrueAsync(() => staying.Received.Count == 3);
        Assert.All(staying.Received, r => Assert.Equal(2, r.Attempt));
    }

    [Fact]
    public async Task Expired_deliveries_are_redelivered()
    {
        await using var broker = TestBroker.Create(o => o.AckTimeout = TimeSpan.FromMilliseconds(1));
        var target = new FakeTarget(broker.NextSubscriptionId());
        var group = broker.Subscribe("t", "g", target, StartPosition.Earliest);
        await broker.PublishAsync("t", Msg(0));
        await Eventually.TrueAsync(() => target.Received.Count == 1);

        await Task.Delay(20);
        Assert.Equal(1, group.ExpireInFlight(DateTimeOffset.UtcNow));
        await Eventually.TrueAsync(() => target.Received.Count == 2);
    }

    [Fact]
    public async Task Latest_start_position_skips_existing_messages()
    {
        await using var broker = TestBroker.Create();
        await broker.PublishAsync("t", Msg(0));
        var target = new FakeTarget(broker.NextSubscriptionId());
        broker.Subscribe("t", "novo", target, StartPosition.Latest);
        await broker.PublishAsync("t", Msg(1));

        await Eventually.TrueAsync(() => target.Received.Count == 1);
        Assert.Equal(1, target.Received.Single().Message.Offset);
    }
}
