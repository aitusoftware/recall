package com.aitusoftware.recall.index;

import org.agrona.BitUtil;

import java.nio.ByteBuffer;
import java.util.function.IntFunction;
import java.util.function.LongConsumer;
import java.util.function.ToIntFunction;

public final class ByteSequenceIndex
{
    private static final int ID_OFFSET = 0;
    private static final int LENGTH_OFFSET = Long.BYTES;
    private static final int USED_INDICATOR_OFFSET = Long.BYTES + Integer.BYTES;
    private static final int DATA_OFFSET = Long.BYTES + Integer.BYTES + Byte.BYTES;
    private final ToIntFunction<ByteBuffer> hash;
    private final float loadFactor = 0.7f;
    private final IntFunction<ByteBuffer> bufferFactory = ByteBuffer::allocate;
    private ByteBuffer dataBuffer;
    private int totalEntryCount;
    private int liveEntryCount;
    private int entryCountToTriggerRehash;
    private int mask;
    private int entrySize;
    private int endOfBuffer;

    public ByteSequenceIndex(final int maxKeyLength, final int initialSize)
    {
        this(maxKeyLength, initialSize, ByteSequenceIndex::defaultHash);
    }

    ByteSequenceIndex(final int maxKeyLength, final int initialSize, final ToIntFunction<ByteBuffer> hash)
    {
        totalEntryCount = BitUtil.findNextPositivePowerOfTwo(initialSize);
        mask = totalEntryCount - 1;
        entrySize = (maxKeyLength + Byte.BYTES + Integer.BYTES + Long.BYTES);
        dataBuffer = bufferFactory.apply(entrySize * totalEntryCount);
        entryCountToTriggerRehash = (int) (loadFactor * totalEntryCount);
        endOfBuffer = totalEntryCount * entrySize;
        this.hash = hash;
    }

    public void insert(final ByteBuffer value, final long id)
    {
        if (liveEntryCount > entryCountToTriggerRehash)
        {
            rehash();
        }
        final int offset = entrySize * (hash.applyAsInt(value) & mask);
        if (isIndexPositionForValue(value, offset))
        {
            insertEntry(value, id, offset);
        }
        else
        {
            for (int i = 1; i < totalEntryCount; i++)
            {
                int candidateOffset = (offset + (i * entrySize));
                if (candidateOffset >= endOfBuffer)
                {
                    candidateOffset -= endOfBuffer;
                }
                if (isIndexPositionForValue(value, candidateOffset))
                {
                    insertEntry(value, id, candidateOffset);
                    return;
                }
            }

            insert(value, id);
        }
    }

    public void search(final ByteBuffer value, final LongConsumer idReceiver)
    {
        int offset = entrySize * (hash.applyAsInt(value) & mask);
        while (dataBuffer.get(offset + USED_INDICATOR_OFFSET) != 0)
        {
            boolean matches = true;
            final int endOfData = value.remaining() + offset + DATA_OFFSET;
            for (int i = offset + DATA_OFFSET; i < endOfData; i++)
            {
                if (dataBuffer.get(i) != value.get(i - offset - DATA_OFFSET))
                {
                    matches = false;
                    break;
                }
            }
            if (matches)
            {
                idReceiver.accept(dataBuffer.getLong(offset + ID_OFFSET));
                return;
            }
            offset += entrySize;
        }
    }

    private void insertEntry(final ByteBuffer value, final long id, final int offset)
    {
        dataBuffer.putLong(offset, id);
        dataBuffer.putInt(offset + LENGTH_OFFSET, value.remaining());
        dataBuffer.put(offset + USED_INDICATOR_OFFSET, (byte) 1);
        final int endOfData = value.remaining() + offset + DATA_OFFSET;
        for (int i = offset + DATA_OFFSET; i < endOfData; i++)
        {
            dataBuffer.put(i, value.get(value.position() + i - offset - DATA_OFFSET));
        }
        liveEntryCount++;
    }

    private void rehash()
    {
        final ByteBuffer oldBuffer = dataBuffer;
        final int oldEntryCount = totalEntryCount;

        dataBuffer = bufferFactory.apply(oldBuffer.capacity() * 2);
        totalEntryCount *= 2;
        mask = totalEntryCount - 1;
        entryCountToTriggerRehash = (int) (loadFactor * totalEntryCount);
        liveEntryCount = 0;
        endOfBuffer = totalEntryCount * entrySize;

        for (int i = 0; i < oldEntryCount; i++)
        {
            int offset = i * entrySize;
            if (oldBuffer.get(offset + USED_INDICATOR_OFFSET) != 0)
            {

                final int valueLength = oldBuffer.getInt(offset + LENGTH_OFFSET);
                oldBuffer.limit(offset + DATA_OFFSET + valueLength)
                        .position(offset + DATA_OFFSET);
                insert(oldBuffer, oldBuffer.getLong(offset + ID_OFFSET));
                oldBuffer.limit(oldBuffer.capacity()).position(0);
            }
        }
    }

    private boolean isIndexPositionForValue(final ByteBuffer value, final int offset)
    {
        return dataBuffer.getInt(offset + USED_INDICATOR_OFFSET) == 0 || isExistingEntry(value, offset);
    }

    private boolean isExistingEntry(final ByteBuffer value, final int offset)
    {
        final int endOfData = value.remaining() + offset + DATA_OFFSET;
        for (int i = offset + DATA_OFFSET; i < endOfData; i++)
        {
            if (dataBuffer.get(i) != value.get(value.position() + i - offset - DATA_OFFSET))
            {
                return false;
            }
        }
        return true;
    }

    private static int defaultHash(final ByteBuffer value)
    {
        final int endOfData = value.remaining() + value.position();
        int hash = 0;
        for (int i = value.position(); i < endOfData; i++)
        {
            hash = (31 * hash) + value.get(i);
        }
        return hash;
    }
}