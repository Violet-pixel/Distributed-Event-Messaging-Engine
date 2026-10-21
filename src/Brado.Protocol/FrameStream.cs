using System.Buffers;
using System.Text;

namespace Brado.Protocol;

public sealed class FrameReader(Stream stream, int maxPayloadBytes = FrameCodec.DefaultMaxPayloadBytes)
{
    private readonly byte[] _header = new byte[FrameCodec.HeaderSize];

    public async ValueTask<Frame?> ReadAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await stream.ReadExactlyAsync(_header, cancellationToken).ConfigureAwait(false);
        }
        catch (EndOfStreamException)
        {
            return null;
        }

        var header = FrameCodec.ReadHeader(_header, maxPayloadBytes);

        var topic = string.Empty;
        if (header.TopicLength > 0)
        {
            var rented = ArrayPool<byte>.Shared.Rent(header.TopicLength);
            try
            {
                await stream.ReadExactlyAsync(rented.AsMemory(0, header.TopicLength), cancellationToken).ConfigureAwait(false);
                topic = Encoding.UTF8.GetString(rented, 0, header.TopicLength);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }

        var payload = header.PayloadLength == 0 ? Array.Empty<byte>() : new byte[header.PayloadLength];
        if (payload.Length > 0)
            await stream.ReadExactlyAsync(payload, cancellationToken).ConfigureAwait(false);

        return new Frame(header.Type, header.CorrelationId, topic, payload, header.Offset, header.Flags);
    }
}

public static class FrameWriter
{
    public static async ValueTask WriteAsync(Stream stream, Frame frame, CancellationToken cancellationToken = default)
    {
        var size = FrameCodec.GetEncodedSize(frame);
        var buffer = ArrayPool<byte>.Shared.Rent(size);
        try
        {
            var written = FrameCodec.Encode(frame, buffer);
            await stream.WriteAsync(buffer.AsMemory(0, written), cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}
