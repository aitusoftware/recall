package com.aitusoftware.recall.store;

import com.aitusoftware.recall.persistence.IdAccessor;
import com.aitusoftware.recall.persistence.Transcoder;
import org.agrona.collections.Long2LongHashMap;

import java.nio.ByteBuffer;

public final class ByteBufferStore implements Store<ByteBuffer>
{
    private static final long NOT_IN_MAP = Long.MIN_VALUE;
    private final ByteBuffer buffer;
    private final ByteBuffer readSlice;
    private final Long2LongHashMap index = new Long2LongHashMap(NOT_IN_MAP);
    private final int internalRecordLength;
    private int nextWriteOffset;

    public ByteBufferStore(final int maxRecordLength, final int maxRecords)
    {
        internalRecordLength = maxRecordLength + Long.BYTES;
        buffer = ByteBuffer.allocateDirect(internalRecordLength * maxRecords);
        readSlice = buffer.slice();
    }

    @Override
    public <T> boolean load(final long id, final ByteBuffer buffer,
                            final Transcoder<ByteBuffer, T> transcoder, final T container)
    {
        final long recordOffset = index.get(id);
        if (recordOffset == NOT_IN_MAP)
        {
            return false;
        }
        readSlice.limit(((int) recordOffset) + internalRecordLength).position((int) recordOffset);
        final long storedId = readSlice.getLong();
        assert storedId == id;
        transcoder.load(readSlice, container);

        return true;
    }

    @Override
    public <T> void store(final ByteBuffer buffer, final Transcoder<ByteBuffer, T> transcoder,
                          final T value, final IdAccessor<T> idAccessor)
    {
        index.put(idAccessor.getId(value), nextWriteOffset);
        this.buffer.limit(nextWriteOffset + internalRecordLength).position(nextWriteOffset);
        this.buffer.putLong(idAccessor.getId(value));
        transcoder.store(this.buffer, value);
        nextWriteOffset += internalRecordLength;
    }
}