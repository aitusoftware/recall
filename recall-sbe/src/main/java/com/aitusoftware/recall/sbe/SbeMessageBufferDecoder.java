package com.aitusoftware.recall.sbe;

import com.aitusoftware.recall.persistence.Decoder;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.sbe.MessageDecoderFlyweight;

public final class SbeMessageBufferDecoder<T extends MessageDecoderFlyweight> implements Decoder<UnsafeBuffer, T>
{
    @Override
    public void load(final UnsafeBuffer buffer, final int offset, final T container)
    {
        container.wrap(buffer, offset, container.sbeBlockLength(),
            container.sbeSchemaVersion());
    }
}
