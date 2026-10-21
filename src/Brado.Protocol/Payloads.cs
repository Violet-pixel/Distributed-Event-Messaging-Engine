using System.Buffers.Binary;
using System.Text;

namespace Brado.Protocol;

public static class StartPosition
{
    public const long Latest = -1;
    public const long Earliest = 0;
}

public readonly record struct SubscribeRequest(string Group, long StartOffset, int Prefetch)
{
    public byte[] ToBytes()
    {
        var groupBytes = Encoding.UTF8.GetByteCount(Group);
        var buffer = new byte[2 + groupBytes + 8 + 4];
        BinaryPrimitives.WriteUInt16BigEndian(buffer, (ushort)groupBytes);
        Encoding.UTF8.GetBytes(Group, buffer.AsSpan(2, groupBytes));
        BinaryPrimitives.WriteInt64BigEndian(buffer.AsSpan(2 + groupBytes), StartOffset);
        BinaryPrimitives.WriteInt32BigEndian(buffer.AsSpan(10 + groupBytes), Prefetch);
        return buffer;
    }

    public static SubscribeRequest Parse(ReadOnlySpan<byte> source)
    {
        if (source.Length < 14)
            throw new ProtocolException("SUBSCRIBE com corpo inválido.");

        var groupLength = BinaryPrimitives.ReadUInt16BigEndian(source);
        if (source.Length < 2 + groupLength + 12)
            throw new ProtocolException("SUBSCRIBE com corpo inválido.");

        var group = Encoding.UTF8.GetString(source.Slice(2, groupLength));
        var start = BinaryPrimitives.ReadInt64BigEndian(source[(2 + groupLength)..]);
        var prefetch = BinaryPrimitives.ReadInt32BigEndian(source[(10 + groupLength)..]);
        return new SubscribeRequest(group, start, prefetch);
    }
}

public static class PayloadCodec
{
    public static byte[] Int64(long value)
    {
        var buffer = new byte[8];
        BinaryPrimitives.WriteInt64BigEndian(buffer, value);
        return buffer;
    }

    public static long ReadInt64(ReadOnlySpan<byte> source) =>
        source.Length < 8 ? throw new ProtocolException("Valor de 8 bytes esperado.") : BinaryPrimitives.ReadInt64BigEndian(source);

    public static string Utf8(ReadOnlyMemory<byte> payload) => Encoding.UTF8.GetString(payload.Span);
}

public static class TopicName
{
    public const int MaxLength = 200;

    public static bool IsValid(string? name)
    {
        if (string.IsNullOrEmpty(name) || name.Length > MaxLength) return false;
        foreach (var c in name)
        {
            var ok = c is (>= 'a' and <= 'z') or (>= 'A' and <= 'Z') or (>= '0' and <= '9') or '.' or '-' or '_';
            if (!ok) return false;
        }
        return true;
    }

    public static void EnsureValid(string? name)
    {
        if (!IsValid(name))
            throw new ProtocolException($"Nome de tópico/grupo inválido: '{name}'.");
    }
}
