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
public final class CharSequenceMap implements SequenceMap<CharSequence>
{
    private static final int KEY_OFFSET = Integer.BYTES * 4;
    private static final int HASH_OFFSET = Integer.BYTES * 3;
    private static final int ID_OFFSET = Integer.BYTES;
    private final ToIntFunction<CharSequence> hash;
    private final CharArrayCharSequence charBuffer = new CharArrayCharSequence();
    private final float loadFactor = 0.7f;
    private final IntFunction<ByteBuffer> bufferFactory;
    private final long missingValue;
    private final int maxKeyLength;
    private final RemoveEntryHandler removeEntryHandler;
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
        this(maxKeyLength, initialSize, CharSequenceMap::defaultHash,
            missingValue, ByteBuffer::allocateDirect);
    }

    /**
     * Constructor for the map.
     *
     * @param maxKeyLength  max length of any key
     * @param initialSize   initial size of the map
     * @param missingValue  value to return if the key is not present
     * @param bufferFactory factory method for creating new {@code ByteBuffer} instances
     */
    public CharSequenceMap(
        final int maxKeyLength, final int initialSize,
        final long missingValue, final IntFunction<ByteBuffer> bufferFactory)
    {
        this(maxKeyLength, initialSize, CharSequenceMap::defaultHash,
            missingValue, bufferFactory);
    }

    CharSequenceMap(
        final int maxKeyLength, final int initialSize,
        final ToIntFunction<CharSequence> hash, final long missingValue,
        final IntFunction<ByteBuffer> bufferFactory)
    {
        if (maxKeyLength > (1 << 28))
        {
            throw new IllegalArgumentException("Key too long");
        }
        totalEntryCount = BitUtil.findNextPositivePowerOfTwo(initialSize);
        entryMask = totalEntryCount - 1;
        entrySizeInBytes = (maxKeyLength * Character.BYTES) + (4 * Integer.BYTES);
        final long bufferSize = entrySizeInBytes * (long)totalEntryCount;
        if (bufferSize > ((long)Integer.MAX_VALUE) || totalEntryCount < 0)
        {
            throw new IllegalArgumentException("Requested buffer size too large");
        }
        this.bufferFactory = bufferFactory;
        dataBuffer = this.bufferFactory.apply((int)bufferSize);
        entryCountToTriggerRehash = (int)(loadFactor * totalEntryCount);
        this.maxKeyLength = maxKeyLength;
        this.hash = hash;
        this.missingValue = missingValue;
        removeEntryHandler = new RemoveEntryHandler(entrySizeInBytes);
    }

    /**
     * {@inheritDoc}.
     */
    public void put(final CharSequence value, final long id)
    {
        if (value.length() > maxKeyLength)
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
        if (isEmptyEntrySlot(entryIndex) || existingEntryAt)
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
                if (isEmptyEntrySlot(candidateIndex) || innerExistingEntryAt)
                {
                    insertEntry(value, id, candidateIndex, !innerExistingEntryAt, hashValue);
                    return;
                }
            }

            put(value, id);
        }
    }

    /**
     * {@inheritDoc}.
     */
    public long get(final CharSequence value)
    {
        return search(value, searchEntryHandler);
    }

    /**
     * {@inheritDoc}.
     */
    public long remove(final CharSequence value)
    {
        return search(value, removeEntryHandler);
    }

    /**
     * {@inheritDoc}.
     */
    public int size()
    {
        return liveEntryCount;
    }

    /**
     * {@inheritDoc}.
     */
    public void rehash()
    {
        rehash(false);
    }

    private void insertEntry(
        final CharSequence value, final long id,
        final int entryIndex, final boolean isInsert, final int hashValue)
    {
        setValueLength(entryIndex, value.length(), dataBuffer);
        final int keyOffset = keyOffset(entryIndex);
        for (int i = 0; i < value.length(); i++)
        {
            dataBuffer.putChar(keyOffset + (i * Character.BYTES), value.charAt(i));
        }
        setId(entryIndex, id, dataBuffer);
        setHash(entryIndex, hashValue, dataBuffer);
        if (isInsert)
        {
            liveEntryCount++;
        }
    }

    private boolean isEmptyEntrySlot(final int entryIndex)
    {
        return !isValuePresent(entryIndex, dataBuffer);
    }

    private void rehash(final boolean shouldResize)
    {
        final ByteBuffer oldBuffer = dataBuffer;
        final int oldEntryCount = totalEntryCount;

        if (shouldResize)
        {
            final int newSize = oldBuffer.capacity() * 2;
            if (newSize < 0)
            {
                throw new IllegalStateException(
                    String.format(
                        "Maximum map capacity exceeded. Entry count: %d, entrySize: %d, newSize: %d",
                        size(), entrySizeInBytes, ((long)oldBuffer.capacity()) * 2));
            }
            dataBuffer = bufferFactory.apply(newSize);
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

                charBuffer.reset(oldBuffer, keyOffset(i), getValueLength(i, oldBuffer));
                put(charBuffer, id);
            }
        }
    }

    private long search(final CharSequence value, final EntryHandler entryHandler)
    {
        final int hashValue = hash.applyAsInt(value);
        int entryIndex = (hashValue & entryMask);
        int entry = 0;
        while (entry < totalEntryCount)
        {
            if (isEmptyEntrySlot(entryIndex) && noDeletes)
            {
                break;
            }

            if (isExistingEntryAt(value, entryIndex, getHash(entryIndex, dataBuffer)))
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

    private boolean isExistingEntryAt(final CharSequence value, final int entryIndex, final int hashValue)
    {
        final int keyOffset = keyOffset(entryIndex);
        final int hash = getHash(entryIndex, dataBuffer);
        if (hash != hashValue)
        {
            return false;
        }
        for (int i = 0; i < value.length(); i++)
        {
            if (dataBuffer.getChar(keyOffset + (i * Character.BYTES)) != value.charAt(i))
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
        dataBuffer.putLong(byteOffset(entryIndex) + ID_OFFSET, id);
    }

    private long getId(final int entryIndex, final ByteBuffer dataBuffer)
    {
        return dataBuffer.getLong(byteOffset(entryIndex) + ID_OFFSET);
    }

    private void setHash(final int entryIndex, final int hashValue, final ByteBuffer dataBuffer)
    {
        dataBuffer.putInt(byteOffset(entryIndex) + HASH_OFFSET, hashValue);
    }

    private int getHash(final int entryIndex, final ByteBuffer dataBuffer)
    {
        return dataBuffer.getInt(byteOffset(entryIndex) + HASH_OFFSET);
    }

    private int keyOffset(final int entryIndex)
    {
        return byteOffset(entryIndex) + KEY_OFFSET;
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
            return dataBuffer.getChar((offset) + (i * Character.BYTES));
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

    private final class RemoveEntryHandler implements EntryHandler
    {
        private final int longsInEntry;
        private final int remainingBytesInEntry;

        private RemoveEntryHandler(final int entrySizeInBytes)
        {
            longsInEntry = entrySizeInBytes / Long.BYTES;
            remainingBytesInEntry = entrySizeInBytes & (Long.BYTES - 1);
        }

        @Override
        public void onEntryFound(final ByteBuffer buffer, final int entryIndex)
        {
            int byteOffset = byteOffset(entryIndex);
            for (int i = 0; i < longsInEntry; i++)
            {
                buffer.putLong(byteOffset, 0L);
                byteOffset += Long.BYTES;
            }
            for (int i = 0; i < remainingBytesInEntry; i++)
            {
                buffer.put(byteOffset++, (byte)0);
            }
            liveEntryCount--;
            noDeletes = false;
        }
    }
}