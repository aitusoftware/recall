package com.aitusoftware.recall.sbe;

import com.aitusoftware.recall.persistence.IdAccessor;
import com.aitusoftware.recall.store.BufferStore;
import com.aitusoftware.recall.store.SingleTypeStore;
import com.aitusoftware.recall.store.UnsafeBufferOps;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.sbe.MessageDecoderFlyweight;

import java.util.function.IntFunction;

public final class SbeMessageStoreFactory
{
    private SbeMessageStoreFactory()
    {
    }

    public static <T extends MessageDecoderFlyweight> SingleTypeStore<UnsafeBuffer, T> forSbeMessage(
        final T decoderFlyweight,
        final int maxMessageLength,
        final int maxRecords,
        final IntFunction<UnsafeBuffer> bufferFactory,
        final IdAccessor<T> idAccessor)
    {
        final BufferStore<UnsafeBuffer> store = new BufferStore<>(
            maxMessageLength, maxRecords, bufferFactory, new UnsafeBufferOps());

        return new SingleTypeStore<>(store, new SbeMessageBufferDecoder<>(),
            new SbeMessageBufferEncoder<>(maxMessageLength), idAccessor);
    }
}