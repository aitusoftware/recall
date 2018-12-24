package com.aitusoftware.recall.store;

import com.aitusoftware.recall.persistence.IdAccessor;
import com.aitusoftware.recall.persistence.Transcoder;
import org.agrona.collections.Long2LongHashMap;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

public final class ByteBufferStore implements Store<ByteBuffer>
{
    private static final long NOT_IN_MAP = Long.MIN_VALUE;
    private final ByteBuffer buffer;
    private final ByteBuffer readSlice;
    private final Long2LongHashMap index = new Long2LongHashMap(NOT_IN_MAP);
    private final Long2LongHashMap removed = new Long2LongHashMap(NOT_IN_MAP);
    private final long[] removedEntryBuffer;
    private final long[] writePositionCache;
    private final int internalRecordLength;
    private final int bufferCapacity;
    private int nextWriteOffset;
    private final Long2LongHashMap writePositionToIdMap = new Long2LongHashMap(NOT_IN_MAP);

    public ByteBufferStore(final int maxRecordLength, final int maxRecords)
    {
        internalRecordLength = maxRecordLength + Long.BYTES;
        buffer = ByteBuffer.allocateDirect(internalRecordLength * maxRecords);
        readSlice = buffer.slice();
        bufferCapacity = buffer.capacity();
        removedEntryBuffer = new long[maxRecords];
        writePositionCache = new long[maxRecords];
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
        assert storedId == id : String.format("stored: %d, requested: %d", storedId, id);
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
            removed.put(id, writeOffset);
        }
        return wasRemoved;
    }

    @Override
    public void compact()
    {
        writePositionToIdMap.clear();
        final Long2LongHashMap.KeyIterator valueKeyIterator = index.keySet().iterator();
        final int numberOfElements = index.size();
        int ptr = 0;
        while (valueKeyIterator.hasNext())
        {
            final long id = valueKeyIterator.nextValue();
            writePositionToIdMap.put(index.get(id), id);
            writePositionCache[ptr++] = index.get(id);
        }
        Arrays.sort(writePositionCache, 0, numberOfElements);

        final int numberOfRemovedElements = removed.size();
        final Long2LongHashMap.KeyIterator removedKeyIterator = removed.keySet().iterator();
        ptr = 0;
        while (removedKeyIterator.hasNext())
        {
            removedEntryBuffer[ptr++] = removed.get(removedKeyIterator.nextValue());
        }
        Arrays.sort(removedEntryBuffer, 0, numberOfRemovedElements);
        final int replacementCount = Math.min(numberOfElements, numberOfRemovedElements);
        ptr = 0;
        for (int i = numberOfElements - 1; i >= numberOfElements - replacementCount; i--)
        {
            final long writePosition = writePositionCache[i];
            final long freePosition = removedEntryBuffer[ptr];
            final long id = writePositionToIdMap.get(writePosition);
            if (freePosition < writePosition)
            {
                for (int j = 0; j < internalRecordLength; j++)
                {
                    buffer.put((int) freePosition + j, buffer.get((int) writePosition + j));
                }
                index.put(id, freePosition);
                removed.remove(id);
                nextWriteOffset = (int) writePosition;
                ptr++;
            }
        }

        while (ptr < numberOfRemovedElements &&
                removedEntryBuffer[ptr] == nextWriteOffset - internalRecordLength)
        {
            nextWriteOffset -= internalRecordLength;
            ptr++;
        }

        index.compact();
        removed.compact();
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
}