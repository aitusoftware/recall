package com.aitusoftware.recall.store;

import com.aitusoftware.recall.persistence.IdAccessor;
import com.aitusoftware.recall.persistence.Transcoder;
import org.agrona.BitUtil;
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
    private final int bufferCapacity;
    private final int numberOfLongsInRecord;
    private final int numberOfTrailingBytesInRecord;
    private int nextWriteOffset;

    public ByteBufferStore(final int maxRecordLength, final int maxRecords)
    {
        internalRecordLength = BitUtil.findNextPositivePowerOfTwo(maxRecordLength + Long.BYTES);
        buffer = ByteBuffer.allocateDirect(internalRecordLength * maxRecords);
        readSlice = buffer.slice();
        bufferCapacity = buffer.capacity();
        numberOfLongsInRecord = maxRecordLength / Long.BYTES;
        numberOfTrailingBytesInRecord = maxRecordLength % Long.BYTES;
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
        assert storedId == id : String.format("stored: %d, requested: %d, at %d", storedId, id, recordOffset);
        transcoder.load(readSlice, container);

        return true;
    }

    @Override
    public <T> void store(final Transcoder<ByteBuffer, T> transcoder,
                          final T value, final IdAccessor<T> idAccessor) throws CapacityExceededException
    {
        if (nextWriteOffset == bufferCapacity)
        {
            throw new CapacityExceededException();
        }
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
    public void grow()
    {

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
        final long retrievedId = buffer.getLong(sourcePosition);
        if (id != retrievedId)
        {
            moveRecord((int) writeOffset, sourcePosition);
            index.put(retrievedId, writeOffset);
        }

        nextWriteOffset -= internalRecordLength;
    }

    private void moveRecord(final int targetPosition, final int sourcePosition)
    {
        for (int j = 0; j < numberOfLongsInRecord; j += Long.BYTES)
        {
            buffer.putLong(targetPosition + j, buffer.getLong(sourcePosition + j));
        }

        for (int j = 0; j < numberOfTrailingBytesInRecord; j++)
        {
            buffer.put(targetPosition + j, buffer.get(sourcePosition + j));
        }
    }
}