package com.aitusoftware.recall.sbe;

import com.aitusoftware.recall.persistence.Encoder;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.sbe.MessageDecoderFlyweight;

public final class SbeMessageBufferEncoder<T extends MessageDecoderFlyweight> implements Encoder<UnsafeBuffer, T>
{
    private final int maxMessageLength;

    public SbeMessageBufferEncoder(final int maxMessageLength)
    {
        this.maxMessageLength = maxMessageLength;
    }

    @Override
    public void store(final UnsafeBuffer buffer, final int offset, final T value)
    {
        final int encodedLength = value.encodedLength();
        if (encodedLength > maxMessageLength)
        {
            throw new IllegalArgumentException("Unable to encode message of length " + encodedLength);
        }
        buffer.putBytes(offset, value.buffer(), value.offset(), encodedLength);
    }
}
