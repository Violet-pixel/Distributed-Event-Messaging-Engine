using System.Buffers.Binary;
using System.Text;

namespace Brado.Protocol;

public static class FrameCodec
{
    public const ushort Magic = 0xA7E5;
    public const byte Version = 1;
    public const int HeaderSize = 28;
    public const int MaxTopicBytes = 512;
    public const int DefaultMaxPayloadBytes = 4 * 1024 * 1024;

    public static int GetEncodedSize(Frame frame) =>
        HeaderSize + Encoding.UTF8.GetByteCount(frame.Topic) + frame.Payload.Length;

    public static int Encode(Frame frame, Span<byte> destination)
    {
        var topicBytes = Encoding.UTF8.GetByteCount(frame.Topic);
        if (topicBytes > MaxTopicBytes)
            throw new ProtocolException($"Tópico excede {MaxTopicBytes} bytes.");

        var total = HeaderSize + topicBytes + frame.Payload.Length;
        if (destination.Length < total)
            throw new ArgumentException("Buffer de destino pequeno demais.", nameof(destination));

        BinaryPrimitives.WriteUInt16BigEndian(destination, Magic);
        destination[2] = Version;
        destination[3] = (byte)frame.Type;
        destination[4] = (byte)frame.Flags;
        destination[5] = 0;
        BinaryPrimitives.WriteInt64BigEndian(destination[6..], frame.CorrelationId);
        BinaryPrimitives.WriteInt64BigEndian(destination[14..], frame.Offset);
        BinaryPrimitives.WriteUInt16BigEndian(destination[22..], (ushort)topicBytes);
        BinaryPrimitives.WriteInt32BigEndian(destination[24..], frame.Payload.Length);

        Encoding.UTF8.GetBytes(frame.Topic, destination.Slice(HeaderSize, topicBytes));
        frame.Payload.Span.CopyTo(destination[(HeaderSize + topicBytes)..]);
        return total;
    }

    public static byte[] Encode(Frame frame)
    {
        var buffer = new byte[GetEncodedSize(frame)];
        Encode(frame, buffer);
        return buffer;
    }

    public static FrameHeader ReadHeader(ReadOnlySpan<byte> source, int maxPayloadBytes = DefaultMaxPayloadBytes)
    {
        if (source.Length < HeaderSize)
            throw new ProtocolException("Cabeçalho incompleto.");

        var magic = BinaryPrimitives.ReadUInt16BigEndian(source);
        if (magic != Magic)
            throw new ProtocolException($"Magic inválido: 0x{magic:X4}.");
        if (source[2] != Version)
            throw new ProtocolException($"Versão de protocolo não suportada: {source[2]}.");

        var type = (FrameType)source[3];
        if (!Enum.IsDefined(type))
            throw new ProtocolException($"Tipo de quadro desconhecido: 0x{source[3]:X2}.");

        var topicLength = BinaryPrimitives.ReadUInt16BigEndian(source[22..]);
        var payloadLength = BinaryPrimitives.ReadInt32BigEndian(source[24..]);

        if (topicLength > MaxTopicBytes)
            throw new ProtocolException("Tópico grande demais.");
        if (payloadLength < 0 || payloadLength > maxPayloadBytes)
            throw new ProtocolException($"Payload fora do limite ({payloadLength} bytes).");

        return new FrameHeader(
            type,
            (FrameFlags)source[4],
            BinaryPrimitives.ReadInt64BigEndian(source[6..]),
            BinaryPrimitives.ReadInt64BigEndian(source[14..]),
            topicLength,
            payloadLength);
    }

    public static Frame Decode(ReadOnlySpan<byte> source, int maxPayloadBytes = DefaultMaxPayloadBytes)
    {
        var header = ReadHeader(source, maxPayloadBytes);
        var bodyLength = header.TopicLength + header.PayloadLength;
        if (source.Length < HeaderSize + bodyLength)
            throw new ProtocolException("Quadro incompleto.");

        var topic = header.TopicLength == 0
            ? string.Empty
            : Encoding.UTF8.GetString(source.Slice(HeaderSize, header.TopicLength));
        var payload = source.Slice(HeaderSize + header.TopicLength, header.PayloadLength).ToArray();

        return new Frame(header.Type, header.CorrelationId, topic, payload, header.Offset, header.Flags);
    }
}
