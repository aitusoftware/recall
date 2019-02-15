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
    private static final int ID_OFFSET = Integer.BYTES;
    private static final int KEY_OFFSET = Integer.BYTES * 4;
    private static final int HASH_OFFSET = Integer.BYTES * 3;
    private final ToIntFunction<ByteBuffer> hash;
    private final float loadFactor = 0.7f;
    private final IntFunction<ByteBuffer> bufferFactory = ByteBuffer::allocate;
    private final long missingValue;
    private final int maxKeyLength;
    private final EntryHandler getEntryHandler = (b, i) -> {};
    private final EntryHandler removeEntryHandler = new RemoveEntryHandler();
    private ByteBuffer dataBuffer;
    private int totalEntryCount;
    private int liveEntryCount;
    private int entryCountToTriggerRehash;
    private int entryMask;
    private int entrySizeInBytes;
    private boolean noDeletes = true;

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
        entryMask = totalEntryCount - 1;
        entrySizeInBytes = (maxKeyLength + Integer.BYTES + Long.BYTES);
        dataBuffer = bufferFactory.apply(entrySizeInBytes * totalEntryCount);
        entryCountToTriggerRehash = (int)(loadFactor * totalEntryCount);
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
            rehash(true);
        }
        final int hashValue = hash.applyAsInt(value);
        final int entryIndex = (hashValue & entryMask);
        final boolean existingEntryAt = isExistingEntryAt(value, entryIndex, hashValue);
        if (!isValuePresent(entryIndex, dataBuffer) || existingEntryAt)
        {
            insertEntry(value, id, entryIndex, !existingEntryAt, hashValue);
        }
        else
        {
            for (int i = 1; i < totalEntryCount; i++)
            {
                int candidateIndex = (entryIndex + (i));
                if (candidateIndex >= totalEntryCount)
                {
                    candidateIndex -= totalEntryCount;
                }
                final boolean innerExistingEntryAt = isExistingEntryAt(value, candidateIndex, hashValue);
                if (!isValuePresent(candidateIndex, dataBuffer) || innerExistingEntryAt)
                {
                    insertEntry(value, id, candidateIndex, !innerExistingEntryAt, hashValue);
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

    /**
     * Allocates a new buffer of the same size, and copies existing entries into it.
     */
    public void rehash()
    {
        rehash(false);
    }

    private long search(final ByteBuffer value, final EntryHandler entryHandler)
    {
        final int hashValue = hash.applyAsInt(value);
        int entryIndex = (hashValue & entryMask);
        int entry = 0;
        while (entry < totalEntryCount)
        {
            if (!isValuePresent(entryIndex, dataBuffer) && noDeletes)
            {
                break;
            }

            boolean matches = true;
            final int keyOffset = keyOffset(entryIndex);
            if (hashValue != getHash(entryIndex, dataBuffer))
            {
                matches = false;
            }
            else
            {
                for (int i = 0; i < value.remaining(); i++)
                {
                    if (dataBuffer.get(keyOffset + i) != value.get(value.position() + (i)))
                    {
                        matches = false;
                        break;
                    }
                }
            }
            if (matches)
            {
                final long storedId = getId(entryIndex, dataBuffer);
                entryHandler.onEntryFound(dataBuffer, entryIndex);
                return storedId;
            }
            entryIndex++;
            entryIndex = entryIndex & entryMask;
            entry++;
        }

        return missingValue;
    }

    private interface EntryHandler
    {
        void onEntryFound(ByteBuffer dataBuffer, int index);
    }

    private void rehash(final boolean shouldResize)
    {
        final ByteBuffer oldBuffer = dataBuffer;
        final int oldEntryCount = totalEntryCount;
        if (shouldResize)
        {
            dataBuffer = bufferFactory.apply(oldBuffer.capacity() * 2);
            totalEntryCount *= 2;
            entryMask = totalEntryCount - 1;
            entryCountToTriggerRehash = (int)(loadFactor * totalEntryCount);
        }
        else
        {
            dataBuffer = bufferFactory.apply(oldBuffer.capacity());
        }

        liveEntryCount = 0;

        for (int i = 0; i < oldEntryCount; i++)
        {
            if (isValuePresent(i, oldBuffer))
            {
                final long id = getId(i, oldBuffer);
                final int sourceOffset = keyOffset(i);
                final int valueLength = getValueLength(i, oldBuffer);
                final int endPosition = sourceOffset + valueLength;
                oldBuffer.limit(endPosition).position(sourceOffset);
                put(oldBuffer, id);
                oldBuffer.limit(oldBuffer.capacity()).position(0);
            }
        }
    }

    private void insertEntry(
        final ByteBuffer value, final long id,
        final int entryIndex, final boolean isInsert, final int hashValue)
    {
        setValueLength(entryIndex, value.remaining(), dataBuffer);
        final int keyOffset = keyOffset(entryIndex);
        for (int i = 0; i < value.remaining(); i++)
        {
            dataBuffer.put(keyOffset + (i), value.get(i + value.position()));
        }
        setId(entryIndex, id, dataBuffer);
        setHash(entryIndex, hashValue, dataBuffer);
        if (isInsert)
        {
            liveEntryCount++;
        }
    }

    private boolean isExistingEntryAt(final ByteBuffer value, final int entryIndex, final int hashValue)
    {
        final int keyOffset = keyOffset(entryIndex);
        if (hashValue != getHash(entryIndex, dataBuffer))
        {
            return false;
        }
        for (int i = 0; i < value.remaining(); i++)
        {
            if (dataBuffer.get(keyOffset + (i)) != value.get(value.position() + i))
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

    private int byteOffset(final int entryIndex)
    {
        return entryIndex * entrySizeInBytes;
    }

    private boolean isValuePresent(final int entryIndex, final ByteBuffer dataBuffer)
    {
        return (0b1000_0000_0000_0000_0000_0000_0000_0000 & dataBuffer.getInt(byteOffset(entryIndex))) != 0;
    }

    private void setValueLength(final int entryIndex, final int valueLength, final ByteBuffer dataBuffer)
    {
        dataBuffer.putInt(byteOffset(entryIndex), 0b1000_0000_0000_0000_0000_0000_0000_0000 | valueLength);
    }

    private int getValueLength(final int entryIndex, final ByteBuffer dataBuffer)
    {
        return (0b0111_1111_1111_1111_1111_1111_1111_1111 & dataBuffer.getInt(byteOffset(entryIndex)));
    }

    private void setId(final int entryIndex, final long id, final ByteBuffer dataBuffer)
    {
        dataBuffer.putLong(byteOffset(entryIndex) + ID_OFFSET, id);
    }

    private long getId(final int entryIndex, final ByteBuffer dataBuffer)
    {
        return dataBuffer.getLong(byteOffset(entryIndex) + ID_OFFSET);
    }

    private void setHash(final int entryIndex, final int hash, final ByteBuffer dataBuffer)
    {
        dataBuffer.putInt(byteOffset(entryIndex) + HASH_OFFSET, hash);
    }

    private long getHash(final int entryIndex, final ByteBuffer dataBuffer)
    {
        return dataBuffer.getInt(byteOffset(entryIndex) + HASH_OFFSET);
    }

    private int keyOffset(final int entryIndex)
    {
        return byteOffset(entryIndex) + KEY_OFFSET;
    }

    private class RemoveEntryHandler implements EntryHandler
    {
        @Override
        public void onEntryFound(final ByteBuffer dataBuffer, final int index)
        {
            final int byteOffset = byteOffset(index);
            for (int i = 0; i < entrySizeInBytes; i++)
            {
                dataBuffer.put(byteOffset + i, (byte)0);
            }
            liveEntryCount--;
            noDeletes = false;
        }
    }
}