package com.aitusoftware.recall.index;

import org.agrona.BitUtil;

import java.util.function.LongConsumer;
import java.util.function.ToIntFunction;

public final class CharSequenceIndex
{
    private final ToIntFunction<CharSequence> hash;
    private final CharArrayCharSequence charBuffer = new CharArrayCharSequence();
    private final float loadFactor = 0.7f;
    private char[] data;
    private int totalEntryCount;
    private int liveEntryCount;
    private int entryCountToTriggerRehash;
    private int mask;
    private int entrySize;
    private int idOffset;

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
        data = new char[totalEntryCount * entrySize];
        entryCountToTriggerRehash = (int) (loadFactor * totalEntryCount);
        this.hash = hash;
    }

    public void insert(final CharSequence value, final long id)
    {
        if (liveEntryCount > entryCountToTriggerRehash)
        {
            rehash();
        }
        final int index = entrySize * (hash.applyAsInt(value) & mask);
        if (data[index] == 0)
        {
            insertEntry(value, id, index);
        }
        else
        {
            for (int i = 1; i < totalEntryCount; i++)
            {
                final int candidateIndex = (index + (i * entrySize));
                // TODO remove modulo
                if (data[candidateIndex % data.length] == 0)
                {
                    insertEntry(value, id, candidateIndex % data.length);
                    return;
                }
            }
            rehash();
            insert(value, id);
        }
    }

    public void search(final CharSequence value, final LongConsumer idReceiver)
    {
        int index = entrySize * (hash.applyAsInt(value) & mask);
        while (data[index] != 0)
        {
            boolean matches = true;
            for (int i = 0; i < value.length(); i++)
            {
                if (data[dataOffset(index) + i] != value.charAt(i))
                {
                    matches = false;
                    break;
                }
            }
            if (matches)
            {
                idReceiver.accept(readId(index));
                return;
            }
            index += entrySize;
        }
    }

    private void insertEntry(final CharSequence value, final long id, final int index)
    {
        data[index] = 1;
        data[lengthOffset(index)] = (char) value.length();
        for (int i = 0; i < value.length(); i++)
        {
            data[dataOffset(index) + i] = value.charAt(i);
        }
        writeId(id, index);
        liveEntryCount++;
    }

    private void rehash()
    {
        final char[] old = data;
        final int oldEntryCount = totalEntryCount;
        data = new char[old.length * 2];
        totalEntryCount *= 2;
        mask = totalEntryCount - 1;
        entryCountToTriggerRehash = (int) (loadFactor * totalEntryCount);

        for (int i = 0; i < oldEntryCount; i++)
        {
            final int index = i * entrySize;
            if (old[index] != 0)
            {
                final long id = readId(index);
                charBuffer.reset(old, dataOffset(index), old[lengthOffset(index)]);
                insert(charBuffer, id);
            }
        }
    }

    private void writeId(final long id, final int index)
    {
        data[index + idOffset] = (char) (id >> 32);
        data[index + idOffset + 1] = (char) id;
    }

    private long readId(final int index)
    {
        return (((long) data[index + idOffset]) << 32) | (int) data[index + idOffset + 1];
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
        private char[] data;
        private int offset;
        private int length;

        void reset(final char[] data, final int offset, final int length)
        {
            this.data = data;
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
            return data[offset + i];
        }

        @Override
        public CharSequence subSequence(final int offset, final int length)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString()
        {
            return new String(data, offset, length);
        }
    }
}