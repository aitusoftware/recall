package com.aitusoftware.recall.store;

import com.aitusoftware.recall.persistence.IdAccessor;
import com.aitusoftware.recall.persistence.Transcoder;
import org.agrona.collections.Long2LongHashMap;

import java.io.OutputStream;
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
    public <T> boolean load(final long id,
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
    public <T> void store(final Transcoder<ByteBuffer, T> transcoder,
                          final T value, final IdAccessor<T> idAccessor)
    {
        final long valueId = idAccessor.getId(value);
        final long existingPosition = index.get(valueId);
        if (existingPosition != NOT_IN_MAP)
        {
            this.buffer.limit((int) existingPosition + internalRecordLength).position((int) existingPosition + Long.BYTES);
        }
        else
        {
            index.put(valueId, nextWriteOffset);
            this.buffer.limit(nextWriteOffset + internalRecordLength).position(nextWriteOffset);
            this.buffer.putLong(valueId);
            nextWriteOffset += internalRecordLength;
        }
        transcoder.store(this.buffer, value);
    }

    @Override
    public boolean remove(final long id)
    {
        return index.remove(id) != NOT_IN_MAP;
    }

    @Override
    public void sync()
    {

    }

    @Override
    public void streamTo(final OutputStream output)
    {

    }

    int getNextWriteOffset()
    {
        return nextWriteOffset;
    }
}