namespace Brado.Protocol;

public enum FrameType : byte
{
    Hello = 0x01,
    HelloAck = 0x02,

    Publish = 0x10,
    PublishAck = 0x11,

    Subscribe = 0x20,
    SubscribeAck = 0x21,
    Unsubscribe = 0x22,

    Deliver = 0x30,
    Ack = 0x31,
    Nack = 0x32,

    Ping = 0x40,
    Pong = 0x41,

    Error = 0x7F
}

[Flags]
public enum FrameFlags : byte
{
    None = 0,

    Requeue = 1 << 0,

    Redelivered = 1 << 1
}

public sealed record Frame(
    FrameType Type,
    long CorrelationId = 0,
    string Topic = "",
    ReadOnlyMemory<byte> Payload = default,
    long Offset = 0,
    FrameFlags Flags = FrameFlags.None)
{
    public static Frame Error(long correlationId, string message) =>
        new(FrameType.Error, correlationId, Payload: System.Text.Encoding.UTF8.GetBytes(message));
}

public readonly record struct FrameHeader(
    FrameType Type,
    FrameFlags Flags,
    long CorrelationId,
    long Offset,
    int TopicLength,
    int PayloadLength);

public sealed class ProtocolException(string message) : Exception(message);
