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

import org.agrona.BitUtil;

import java.nio.ByteBuffer;
import java.util.function.IntFunction;
import java.util.function.ToIntFunction;

/**
 * Map for storing a char-sequence against a <code>long</code> value.
 */
public final class CharSequenceMap
{
    private final ToIntFunction<CharSequence> hash;
    private final CharArrayCharSequence charBuffer = new CharArrayCharSequence();
    private final float loadFactor = 0.7f;
    private final IntFunction<ByteBuffer> bufferFactory = ByteBuffer::allocate;
    private final long missingValue;
    private final int maxKeyLength;
    private final RemoveEntryHandler removeEntryHandler = new RemoveEntryHandler();
    private final EntryHandler searchEntryHandler = (b, i) -> {};
    private final int entrySizeInBytes;
    private ByteBuffer dataBuffer;
    private int totalEntryCount;
    private int liveEntryCount;
    private int entryCountToTriggerRehash;
    private int entryMask;
    private boolean noDeletes = true;

    /**
     * Constructor for the map.
     *
     * @param maxKeyLength max length of any key
     * @param initialSize  initial size of the map
     * @param missingValue value to return if the key is not present
     */
    public CharSequenceMap(final int maxKeyLength, final int initialSize, final long missingValue)
    {
        this(maxKeyLength, initialSize, CharSequenceMap::defaultHash, missingValue);
    }

    CharSequenceMap(
        final int maxKeyLength, final int initialSize,
        final ToIntFunction<CharSequence> hash, final long missingValue)
    {
        if (maxKeyLength > (1 << 28))
        {
            throw new IllegalArgumentException("Key too long");
        }
        totalEntryCount = BitUtil.findNextPositivePowerOfTwo(initialSize);
        entryMask = totalEntryCount - 1;
        final int entrySizeInInts = (maxKeyLength + 3);
        entrySizeInBytes = entrySizeInInts * Integer.BYTES;
        dataBuffer = bufferFactory.apply((entrySizeInInts * Integer.BYTES) * totalEntryCount);
        entryCountToTriggerRehash = (int)(loadFactor * totalEntryCount);
        this.maxKeyLength = maxKeyLength;
        this.hash = hash;
        this.missingValue = missingValue;
    }

    /**
     * Insert a value into the map.
     *
     * @param value value to use as a key
     * @param id    id to store
     */
    public void put(final CharSequence value, final long id)
    {
        if (value.length() > maxKeyLength)
        {
            throw new IllegalArgumentException("Key too long");
        }
        if (liveEntryCount > entryCountToTriggerRehash)
        {
            rehash();
        }
        final int entryIndex = (hash.applyAsInt(value) & entryMask);

        if (isIndexPositionForValue(value, entryIndex))
        {
            insertEntry(value, id, entryIndex);
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
                if (isIndexPositionForValue(value, candidateIndex))
                {
                    insertEntry(value, id, candidateIndex);
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
     * @return the retrieved value, or {@code missingValue} if it was not present
     */
    public long get(final CharSequence value)
    {
        return search(value, searchEntryHandler);
    }

    /**
     * Removes an entry for a given key.
     *
     * @param value the key to search for
     * @return the stored value, or {@code missingValue} if the key was not present
     */
    public long remove(final CharSequence value)
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

    private void insertEntry(final CharSequence value, final long id, final int entryIndex)
    {
        setValueLength(entryIndex, value.length(), dataBuffer);
        final int keyOffset = keyOffset(entryIndex);
        for (int i = 0; i < value.length(); i++)
        {
            dataBuffer.putInt(keyOffset + (i * Integer.BYTES), value.charAt(i));
        }
        setId(entryIndex, id, dataBuffer);
        liveEntryCount++;
    }

    private void rehash()
    {
        final ByteBuffer oldBuffer = dataBuffer;
        final int oldEntryCount = totalEntryCount;

        dataBuffer = bufferFactory.apply(oldBuffer.capacity() * 2);
        totalEntryCount *= 2;
        entryMask = totalEntryCount - 1;
        entryCountToTriggerRehash = (int)(loadFactor * totalEntryCount);
        liveEntryCount = 0;

        for (int i = 0; i < oldEntryCount; i++)
        {
            if (isValuePresent(i, oldBuffer))
            {
                final long id = getId(i, oldBuffer);

                charBuffer.reset(oldBuffer, keyOffset(i), getValueLength(i, oldBuffer));
                put(charBuffer, id);
            }
        }
    }

    private long search(final CharSequence value, final EntryHandler entryHandler)
    {
        int entryIndex = (hash.applyAsInt(value) & entryMask);
        int entry = 0;
        while (entry < totalEntryCount)
        {
            if (!isValuePresent(entryIndex, dataBuffer) && noDeletes)
            {
                break;
            }

            boolean matches = true;

            final int keyOffset = keyOffset(entryIndex);
            for (int i = 0; i < value.length(); i++)
            {
                if (dataBuffer.getInt(keyOffset + i * Integer.BYTES) != value.charAt(i))
                {
                    matches = false;
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

    private boolean isIndexPositionForValue(final CharSequence value, final int entryIndex)
    {
        return !isValuePresent(entryIndex, dataBuffer) || isExistingEntryAt(value, entryIndex);
    }

    private boolean isExistingEntryAt(final CharSequence value, final int entryIndex)
    {
        final int keyOffset = keyOffset(entryIndex);
        for (int i = 0; i < value.length(); i++)
        {
            if (dataBuffer.getInt(keyOffset + (i * Integer.BYTES)) != value.charAt(i))
            {
                return false;
            }
        }
        return true;
    }

    private static int defaultHash(final CharSequence value)
    {
        int hash = 0;
        for (int i = 0; i < value.length(); i++)
        {
            hash = (31 * hash) + value.charAt(i);
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
        dataBuffer.putLong(byteOffset(entryIndex) + Integer.BYTES, id);
    }

    private long getId(final int entryIndex, final ByteBuffer dataBuffer)
    {
        return dataBuffer.getLong(byteOffset(entryIndex) + Integer.BYTES);
    }

    private int keyOffset(final int entryIndex)
    {
        return byteOffset(entryIndex) + Integer.BYTES * 3;
    }

    private static final class CharArrayCharSequence implements CharSequence
    {
        private ByteBuffer dataBuffer;
        private int offset;
        private int length;

        void reset(final ByteBuffer dataBuffer, final int offset, final int length)
        {
            this.dataBuffer = dataBuffer;
            this.offset = offset;
            this.length = length;
        }

        @Override
        public int length()
        {
            return length;
        }

        @Override
        public char charAt(final int i)
        {
            return (char)dataBuffer.getInt((offset) + (i * Integer.BYTES));
        }

        @Override
        public CharSequence subSequence(final int offset, final int length)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString()
        {
            final StringBuilder builder = new StringBuilder();
            for (int i = 0; i < length; i++)
            {
                builder.append(charAt(i));
            }
            return builder.toString();
        }
    }

    private class RemoveEntryHandler implements EntryHandler
    {
        @Override
        public void onEntryFound(final ByteBuffer buffer, final int entryIndex)
        {
            final int byteOffset = byteOffset(entryIndex);
            for (int i = 0; i < entrySizeInBytes; i++)
            {
                buffer.put(byteOffset + i, (byte)0);
            }
            liveEntryCount--;
            noDeletes = false;
        }
    }
}