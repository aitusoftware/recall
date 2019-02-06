/*
 * Copyright 2019 Aitu Software Limited.
 *
 * https://aitusoftware.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.aitusoftware.recall.map;

import java.nio.ByteBuffer;
import java.util.function.IntFunction;
import java.util.function.ToIntFunction;

import org.agrona.BitUtil;

/**
 * Map for storing a byte-sequence against a <code>long</code> value.
 */
public final class ByteSequenceMap
{
    private static final int ID_OFFSET = 0;
    private static final int LENGTH_OFFSET = Long.BYTES;
    private static final int USED_INDICATOR_OFFSET = Long.BYTES + Integer.BYTES;
    private static final int DATA_OFFSET = Long.BYTES + Integer.BYTES + Byte.BYTES;
    private final ToIntFunction<ByteBuffer> hash;
    private final float loadFactor = 0.7f;
    private final IntFunction<ByteBuffer> bufferFactory = ByteBuffer::allocate;
    private final long missingValue;
    private final int maxKeyLength;
    private ByteBuffer dataBuffer;
    private int totalEntryCount;
    private int liveEntryCount;
    private int entryCountToTriggerRehash;
    private int mask;
    private int entrySize;
    private int endOfBuffer;
    private final EntryHandler getEntryHandler = (b, i) -> {};
    private final EntryHandler removeEntryHandler = new RemoveEntryHandler();

    /**
     * Constructor for the map.
     *
     * @param maxKeyLength max length of any key
     * @param initialSize  initial size of the map
     * @param missingValue initial size of the map
     */
    public ByteSequenceMap(final int maxKeyLength, final int initialSize, final long missingValue)
    {
        this(maxKeyLength, initialSize, ByteSequenceMap::defaultHash, missingValue);
    }

    ByteSequenceMap(
        final int maxKeyLength, final int initialSize,
        final ToIntFunction<ByteBuffer> hash, final long missingValue)
    {
        totalEntryCount = BitUtil.findNextPositivePowerOfTwo(initialSize);
        mask = totalEntryCount - 1;
        entrySize = (maxKeyLength + Byte.BYTES + Integer.BYTES + Long.BYTES);
        dataBuffer = bufferFactory.apply(entrySize * totalEntryCount);
        entryCountToTriggerRehash = (int)(loadFactor * totalEntryCount);
        endOfBuffer = totalEntryCount * entrySize;
        this.hash = hash;
        this.missingValue = missingValue;
        this.maxKeyLength = maxKeyLength;
    }

    /**
     * Insert a value into the map.
     *
     * @param value value to use as a key
     * @param id    id to store
     */
    public void put(final ByteBuffer value, final long id)
    {
        if (value.remaining() > maxKeyLength)
        {
            throw new IllegalArgumentException("Key too long");
        }
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

            put(value, id);
        }
    }

    /**
     * Searches the map for a given key.
     *
     * @param value the key to search for
     * @return the retrieved value or {@code missingValue} if it was not found
     */
    public long get(final ByteBuffer value)
    {
        return search(value, getEntryHandler);
    }

    /**
     * Removes an entry for a given key.
     *
     * @param value the key to search for
     * @return the stored value, or {@code missingValue} if the key was not present
     */
    public long remove(final ByteBuffer value)
    {
        return search(value, removeEntryHandler);
    }

    /**
     * Returns the number of entries in the map.
     *
     * @return the number of entries
     */
    public int size()
    {
        return liveEntryCount;
    }

    private long search(final ByteBuffer value, final EntryHandler entryHandler)
    {
        int index = entrySize * (hash.applyAsInt(value) & mask);
        int entry = 0;
        while (entry < totalEntryCount)
        {
            if (dataBuffer.get((index + USED_INDICATOR_OFFSET) % dataBuffer.capacity()) == 0)
            {
                break;
            }

            boolean matches = true;
            final int endOfData = value.remaining() + index + DATA_OFFSET;
            for (int i = index + DATA_OFFSET; i < endOfData; i++)
            {
                if (dataBuffer.get(i % dataBuffer.capacity()) != value.get((i - index - DATA_OFFSET)))
                {
                    matches = false;
                    break;
                }
            }
            if (matches)
            {
                final long storedId = dataBuffer.getLong((index + ID_OFFSET) % dataBuffer.capacity());
                entryHandler.onEntryFound(dataBuffer, index);
                return storedId;
            }
            index += entrySize;
            entry++;
        }

        return missingValue;
    }

    private interface EntryHandler
    {
        void onEntryFound(ByteBuffer dataBuffer, int index);
    }

    private void insertEntry(final ByteBuffer value, final long id, final int offset)
    {
        dataBuffer.putLong(offset, id);
        dataBuffer.putInt(offset + LENGTH_OFFSET, value.remaining());
        dataBuffer.put(offset + USED_INDICATOR_OFFSET, (byte)1);
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
        entryCountToTriggerRehash = (int)(loadFactor * totalEntryCount);
        liveEntryCount = 0;
        endOfBuffer = totalEntryCount * entrySize;

        for (int i = 0; i < oldEntryCount; i++)
        {
            final int offset = i * entrySize;
            if (oldBuffer.get(offset + USED_INDICATOR_OFFSET) != 0)
            {

                final int valueLength = oldBuffer.getInt(offset + LENGTH_OFFSET);
                oldBuffer.limit(offset + DATA_OFFSET + valueLength)
                        .position(offset + DATA_OFFSET);
                put(oldBuffer, oldBuffer.getLong(offset + ID_OFFSET));
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

    private class RemoveEntryHandler implements EntryHandler
    {
        @Override
        public void onEntryFound(final ByteBuffer dataBuffer, final int index)
        {
            dataBuffer.putLong(index, 0);
            dataBuffer.putInt(index + LENGTH_OFFSET, 0);
            dataBuffer.put(index + USED_INDICATOR_OFFSET, (byte)1);
            final int endOfData = maxKeyLength + index + DATA_OFFSET;
            for (int i = index + DATA_OFFSET; i < endOfData; i++)
            {
                dataBuffer.put(i, (byte)0);
            }
            liveEntryCount--;
        }
    }
}