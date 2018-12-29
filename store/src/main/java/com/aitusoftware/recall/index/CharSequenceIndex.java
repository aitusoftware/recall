package com.aitusoftware.recall.index;

import org.agrona.BitUtil;

import java.nio.ByteBuffer;
import java.util.function.IntFunction;
import java.util.function.LongConsumer;
import java.util.function.ToIntFunction;

public final class CharSequenceIndex
{
    private final ToIntFunction<CharSequence> hash;
    private final CharArrayCharSequence charBuffer = new CharArrayCharSequence();
    private final float loadFactor = 0.7f;
    private final IntFunction<ByteBuffer> bufferFactory = ByteBuffer::allocate;
    private ByteBuffer dataBuffer;
    private int totalEntryCount;
    private int liveEntryCount;
    private int entryCountToTriggerRehash;
    private int mask;
    private int entrySize;
    private int idOffset;
    private int maxCandidateIndex;

    public CharSequenceIndex(final int maxKeyLength, final int initialSize)
    {
        this(maxKeyLength, initialSize, CharSequenceIndex::defaultHash);
    }

    CharSequenceIndex(final int maxKeyLength, final int initialSize, final ToIntFunction<CharSequence> hash)
    {
        totalEntryCount = BitUtil.findNextPositivePowerOfTwo(initialSize);
        mask = totalEntryCount - 1;
        entrySize = (maxKeyLength + 3);
        idOffset = maxKeyLength + 1;
        dataBuffer = bufferFactory.apply(((maxKeyLength + 3) * Integer.BYTES) * totalEntryCount);
        entryCountToTriggerRehash = (int) (loadFactor * totalEntryCount);
        maxCandidateIndex = totalEntryCount * entrySize;
        this.hash = hash;
    }

    public void insert(final CharSequence value, final long id)
    {
        if (liveEntryCount > entryCountToTriggerRehash)
        {
            rehash();
        }
        final int index = entrySize * (hash.applyAsInt(value) & mask);
        if (isIndexPositionForValue(value, index))
        {
            insertEntry(value, id, index);
        }
        else
        {
            for (int i = 1; i < totalEntryCount; i++)
            {
                int candidateIndex = (index + (i * entrySize));
                if (candidateIndex >= maxCandidateIndex)
                {
                    candidateIndex -= maxCandidateIndex;
                }
                if (isIndexPositionForValue(value, candidateIndex))
                {
                    insertEntry(value, id, candidateIndex);
                    return;
                }
            }

            insert(value, id);
        }
    }

    private boolean isIndexPositionForValue(final CharSequence value, final int index)
    {
        return dataBuffer.getInt(index * Integer.BYTES) == 0 || isExistingEntry(value, index);
    }

    public void search(final CharSequence value, final LongConsumer idReceiver)
    {
        int index = entrySize * (hash.applyAsInt(value) & mask);
        while (dataBuffer.getInt(index * Integer.BYTES) != 0)
        {
            boolean matches = true;
            for (int i = 0; i < value.length(); i++)
            {
                if (dataBuffer.getInt((dataOffset(index) + i) * Integer.BYTES) != value.charAt(i))
                {
                    matches = false;
                    break;
                }
            }
            if (matches)
            {
                idReceiver.accept(readId(index, dataBuffer));
                return;
            }
            index += entrySize;
        }
    }

    private void insertEntry(final CharSequence value, final long id, final int index)
    {
        final int byteOffset = index * Integer.BYTES;
        dataBuffer.putInt(byteOffset, 1);
        dataBuffer.putInt(lengthOffset(index) * Integer.BYTES, value.length());
        for (int i = 0; i < value.length(); i++)
        {
            dataBuffer.putInt(byteOffset + (2 * Integer.BYTES) + (i * Integer.BYTES), value.charAt(i));
        }

        writeId(id, index);
        liveEntryCount++;
    }

    private boolean isExistingEntry(final CharSequence value, final int index)
    {
        for (int i = 0; i < value.length(); i++)
        {
            if (dataBuffer.getInt((dataOffset(index) + i) * Integer.BYTES) != value.charAt(i))
            {
                return false;
            }
        }
        return true;
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
        maxCandidateIndex = totalEntryCount * entrySize;

        for (int i = 0; i < oldEntryCount; i++)
        {
            final int index = i * entrySize;
            if (oldBuffer.getInt(index * Integer.BYTES) != 0)
            {
                final long id = readId(index, oldBuffer);
                charBuffer.reset(oldBuffer, dataOffset(index), oldBuffer.getInt(lengthOffset(index) * Integer.BYTES));
                insert(charBuffer, id);
            }
        }
    }

    private void writeId(final long id, final int index)
    {
        dataBuffer.putLong((index + idOffset) * Integer.BYTES, id);
    }

    private long readId(final int index, final ByteBuffer backingBuffer)
    {
        return backingBuffer.getLong((index + idOffset) * Integer.BYTES);
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

    private static int dataOffset(final int index)
    {
        return index + 2;
    }

    // TODO - length could indicate presence, set 0th element to -1 on start
    private static int lengthOffset(final int index)
    {
        return index + 1;
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
            return (char) dataBuffer.getInt((offset + i) * Integer.BYTES);
        }

        @Override
        public CharSequence subSequence(final int offset, final int length)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString()
        {
            return CharArrayCharSequence.class.getSimpleName();
        }
    }
}