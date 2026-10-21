using System.Buffers.Binary;
using System.Text;
using Brado.Protocol;

namespace Brado.UnitTests;

public sealed class ProtocolTests
{
    [Fact]
    public void Encode_then_decode_preserves_every_field()
    {
        var payload = Encoding.UTF8.GetBytes("{\"temperatura\":21.5}");
        var frame = new Frame(FrameType.Deliver, 42, "telemetria.sensores", payload, 1_234_567, FrameFlags.Redelivered);

        var decoded = FrameCodec.Decode(FrameCodec.Encode(frame));

        Assert.Equal(frame.Type, decoded.Type);
        Assert.Equal(frame.CorrelationId, decoded.CorrelationId);
        Assert.Equal(frame.Topic, decoded.Topic);
        Assert.Equal(frame.Offset, decoded.Offset);
        Assert.Equal(frame.Flags, decoded.Flags);
        Assert.Equal(payload, decoded.Payload.ToArray());
    }

    [Fact]
    public void Encoded_size_matches_header_topic_and_payload()
    {
        var frame = new Frame(FrameType.Publish, Topic: "pedidos", Payload: new byte[100]);
        Assert.Equal(FrameCodec.HeaderSize + 7 + 100, FrameCodec.GetEncodedSize(frame));
        Assert.Equal(FrameCodec.GetEncodedSize(frame), FrameCodec.Encode(frame).Length);
    }

    [Fact]
    public void Header_with_wrong_magic_is_rejected()
    {
        var bytes = FrameCodec.Encode(new Frame(FrameType.Ping));
        bytes[0] = 0x00;
        Assert.Throws<ProtocolException>(() => FrameCodec.ReadHeader(bytes));
    }

    [Fact]
    public void Payload_above_limit_is_rejected_before_allocation()
    {
        var bytes = FrameCodec.Encode(new Frame(FrameType.Publish, Topic: "t", Payload: new byte[10]));
        BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(24), 64 * 1024 * 1024);
        Assert.Throws<ProtocolException>(() => FrameCodec.ReadHeader(bytes, maxPayloadBytes: 1024));
    }

    [Fact]
    public void Unknown_frame_type_is_rejected()
    {
        var bytes = FrameCodec.Encode(new Frame(FrameType.Ping));
        bytes[3] = 0x66;
        Assert.Throws<ProtocolException>(() => FrameCodec.ReadHeader(bytes));
    }

    [Fact]
    public async Task Reader_returns_frames_in_order_and_null_at_end_of_stream()
    {
        var stream = new MemoryStream();
        await FrameWriter.WriteAsync(stream, new Frame(FrameType.Publish, 1, "a", new byte[] { 1, 2, 3 }));
        await FrameWriter.WriteAsync(stream, new Frame(FrameType.Ping, 2));
        stream.Position = 0;

        var reader = new FrameReader(stream);
        var first = await reader.ReadAsync();
        var second = await reader.ReadAsync();
        var end = await reader.ReadAsync();

        Assert.Equal(FrameType.Publish, first!.Type);
        Assert.Equal(new byte[] { 1, 2, 3 }, first.Payload.ToArray());
        Assert.Equal(FrameType.Ping, second!.Type);
        Assert.Equal(2, second.CorrelationId);
        Assert.Null(end);
    }

    [Fact]
    public void Subscribe_request_roundtrips()
    {
        var request = new SubscribeRequest("analytics", StartPosition.Earliest, 128);
        Assert.Equal(request, SubscribeRequest.Parse(request.ToBytes()));
    }

    [Theory]
    [InlineData("orders.events", true)]
    [InlineData("telemetria_sensores-v2", true)]
    [InlineData("", false)]
    [InlineData("com espaço", false)]
    [InlineData("barra/invalida", false)]
    public void Topic_names_are_validated(string name, bool valid) =>
        Assert.Equal(valid, TopicName.IsValid(name));
}
