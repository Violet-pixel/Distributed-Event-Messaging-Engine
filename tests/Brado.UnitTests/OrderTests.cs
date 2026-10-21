using Brado.Domain;
using Brado.Domain.Events;

namespace Brado.UnitTests;

public sealed class OrderTests
{
    private static Order SubmittedOrder()
    {
        var order = Order.Create(Guid.NewGuid(), "CLI-1001");
        order.AddItem("liv-arq-001", 2, 129.90m);
        order.AddItem("LIV-ALG-003", 1, 99.50m);
        order.Submit();
        return order;
    }

    [Fact]
    public void Happy_path_goes_from_draft_to_shipped()
    {
        var order = SubmittedOrder();
        order.RecordPayment("PAY-1", order.Total);
        order.ReserveStock("RSV-1");
        order.Ship("BR123BD");

        Assert.Equal(OrderStatus.Shipped, order.Status);
        Assert.Equal(359.30m, order.Total);
        Assert.Equal("LIV-ARQ-001", order.Lines[0].Sku);
        Assert.Equal(7, order.UncommittedEvents.Count);
    }

    [Fact]
    public void Empty_order_cannot_be_submitted()
    {
        var order = Order.Create(Guid.NewGuid(), "CLI-1");
        Assert.Throws<DomainException>(order.Submit);
    }

    [Fact]
    public void Payment_must_match_total()
    {
        var order = SubmittedOrder();
        Assert.Throws<DomainException>(() => order.RecordPayment("PAY-1", 1m));
    }

    [Fact]
    public void Paid_order_cannot_be_cancelled_before_refund()
    {
        var order = SubmittedOrder();
        order.RecordPayment("PAY-1", order.Total);
        order.RecordStockFailure("sem estoque");

        Assert.Throws<DomainException>(() => order.Cancel("sem estoque"));

        order.RefundPayment("REF-1");
        order.Cancel("sem estoque");
        Assert.Equal(OrderStatus.Cancelled, order.Status);
        Assert.True(order.Refunded);
    }

    [Fact]
    public void Items_cannot_be_added_after_submit()
    {
        var order = SubmittedOrder();
        Assert.Throws<DomainException>(() => order.AddItem("X", 1, 1m));
    }

    [Fact]
    public void Rehydrating_from_history_rebuilds_the_same_state()
    {
        var original = SubmittedOrder();
        original.RecordPayment("PAY-9", original.Total);

        var rebuilt = new Order();
        rebuilt.LoadFromHistory(original.UncommittedEvents);

        Assert.Equal(original.Id, rebuilt.Id);
        Assert.Equal(original.Status, rebuilt.Status);
        Assert.Equal(original.Total, rebuilt.Total);
        Assert.Equal(original.PaymentRef, rebuilt.PaymentRef);
        Assert.Equal(original.UncommittedEvents.Count, rebuilt.Version);
        Assert.Empty(rebuilt.UncommittedEvents);
    }

    [Fact]
    public void Snapshot_restores_state_and_version()
    {
        var order = SubmittedOrder();
        order.MarkCommitted();
        var snapshotable = (ISnapshotable)order;

        var restored = new Order();
        ((ISnapshotable)restored).RestoreSnapshot(snapshotable.TakeSnapshot(), order.Version);

        Assert.Equal(order.Id, restored.Id);
        Assert.Equal(OrderStatus.Submitted, restored.Status);
        Assert.Equal(order.Lines.Count, restored.Lines.Count);
        Assert.Equal(order.Version, restored.Version);
    }

    [Fact]
    public void Commands_raise_the_expected_event_types()
    {
        var order = SubmittedOrder();
        Assert.Collection(order.UncommittedEvents,
            e => Assert.IsType<OrderCreated>(e),
            e => Assert.IsType<OrderItemAdded>(e),
            e => Assert.IsType<OrderItemAdded>(e),
            e => Assert.IsType<OrderSubmitted>(e));
    }
}
