package com.aitusoftware.recall.store;

import com.aitusoftware.recall.persistence.Decoder;
import com.aitusoftware.recall.persistence.Encoder;
import com.aitusoftware.recall.persistence.IdAccessor;
import org.agrona.collections.Long2LongHashMap;

import java.io.OutputStream;
import java.util.function.IntFunction;

public final class BufferStore<B> implements Store<B>
{
    private static final long NOT_IN_MAP = Long.MIN_VALUE;
    private final B buffer;
    private final Long2LongHashMap index = new Long2LongHashMap(NOT_IN_MAP);
    private final int internalRecordLength;
    private final int bufferCapacity;
    private final BufferOps<B> bufferOps;
    private int nextWriteOffset;

    public BufferStore(
        final int maxRecordLength, final int maxRecords,
        final IntFunction<B> bufferFactory,
        final BufferOps<B> bufferOps)
    {
        internalRecordLength = maxRecordLength + Long.BYTES;
        bufferCapacity = internalRecordLength * maxRecords;
        this.bufferOps = bufferOps;
        buffer = bufferFactory.apply(bufferCapacity);
    }

    @Override
    public <T> boolean load(
        final long id, final Decoder<B, T> decoder, final T container)
    {
        final long recordOffset = index.get(id);
        if (recordOffset == NOT_IN_MAP)
        {
            return false;
        }
        final long storedId = bufferOps.readLong(buffer, (int)recordOffset);
        assert storedId == id : String.format("stored: %d, requested: %d, at %d", storedId, id, recordOffset);
        decoder.load(buffer, (int)recordOffset + Long.BYTES, container);

        return true;
    }

    @Override
    public <T> void store(
        final Encoder<B, T> encoder, final T value, final IdAccessor<T> idAccessor)
        throws CapacityExceededException
    {
        if (nextWriteOffset == bufferCapacity)
        {
            throw new CapacityExceededException();
        }
        final long valueId = idAccessor.getId(value);
        final long existingPosition = index.get(valueId);
        final int recordWriteOffset;
        if (existingPosition != NOT_IN_MAP)
        {
            recordWriteOffset = (int)existingPosition + Long.BYTES;
        }
        else
        {
            index.put(valueId, nextWriteOffset);
            bufferOps.writeLong(buffer, nextWriteOffset, valueId);
            recordWriteOffset = nextWriteOffset + Long.BYTES;
            nextWriteOffset += internalRecordLength;
        }
        encoder.store(this.buffer, recordWriteOffset, value);
    }

    @Override
    public boolean remove(final long id)
    {
        final long writeOffset = index.remove(id);
        final boolean wasRemoved = writeOffset != NOT_IN_MAP;
        if (wasRemoved)
        {
            moveLastWrittenEntryTo(id, writeOffset);
        }
        return wasRemoved;
    }

    @Override
    public void compact()
    {
        index.compact();
    }

    @Override
    public void sync()
    {

    }

    @Override
    public void streamTo(final OutputStream output)
    {

    }

    @Override
    public float utilisation()
    {
        return 0;
    }

    int nextWriteOffset()
    {
        return nextWriteOffset;
    }

    private void moveLastWrittenEntryTo(final long id, final long writeOffset)
    {
        final int sourcePosition = nextWriteOffset - internalRecordLength;
        final long retrievedId = bufferOps.readLong(buffer, sourcePosition);
        if (id != retrievedId)
        {
            moveRecord((int)writeOffset, sourcePosition);
            index.put(retrievedId, writeOffset);
        }

        nextWriteOffset -= internalRecordLength;
    }

    private void moveRecord(final int targetPosition, final int sourcePosition)
    {
        bufferOps.copyBytes(buffer, buffer, sourcePosition, targetPosition, internalRecordLength);
    }
}