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
package com.aitusoftware.recall.store;

import com.aitusoftware.recall.persistence.Decoder;
import com.aitusoftware.recall.persistence.Encoder;
import com.aitusoftware.recall.persistence.IdAccessor;
import org.agrona.collections.Long2LongHashMap;

import java.io.OutputStream;
import java.util.function.IntFunction;

/**
 * Implementation of {@link Store} that serialises data to a buffer of type <code>B</code>.
 *
 * @param <B> type of the underlying buffer
 */
public final class BufferStore<B> implements Store<B>
{
    private static final long NOT_IN_MAP = Long.MIN_VALUE;
    private final Long2LongHashMap index = new Long2LongHashMap(NOT_IN_MAP);
    private final int internalRecordLength;
    private final BufferOps<B> bufferOps;
    private final IntFunction<B> bufferFactory;
    private int bufferCapacity;
    private B buffer;
    private int nextWriteOffset;
    private int size;

    /**
     * Constructor for the BufferStore.
     *
     * @param maxRecordLength max length of any record
     * @param maxRecords      max number of records that need to be stored
     * @param bufferFactory   provider for the underlying buffer type
     * @param bufferOps       provider of operations on the underlying buffer type
     */
    public BufferStore(
        final int maxRecordLength, final int maxRecords,
        final IntFunction<B> bufferFactory,
        final BufferOps<B> bufferOps)
    {
        internalRecordLength = maxRecordLength + Long.BYTES;
        bufferCapacity = internalRecordLength * maxRecords;
        this.bufferOps = bufferOps;
        this.bufferFactory = bufferFactory;
        buffer = this.bufferFactory.apply(bufferCapacity);
    }

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> void store(
        final Encoder<B, T> encoder, final T value, final IdAccessor<T> idAccessor)
    {
        if (nextWriteOffset == bufferCapacity)
        {
            final B expandedBuffer = bufferFactory.apply(bufferCapacity << 1);
            bufferOps.copyBytes(buffer, expandedBuffer, 0, 0, bufferCapacity);
            buffer = expandedBuffer;
            bufferCapacity <<= 1;
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
            size++;
        }
        encoder.store(this.buffer, recordWriteOffset, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean remove(final long id)
    {
        final long writeOffset = index.remove(id);
        final boolean wasRemoved = writeOffset != NOT_IN_MAP;
        if (wasRemoved)
        {
            moveLastWrittenEntryTo(id, writeOffset);
            size--;
        }
        return wasRemoved;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void compact()
    {
        index.compact();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sync()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void streamTo(final OutputStream output)
    {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public float utilisation()
    {
        return nextWriteOffset / (float)bufferCapacity;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int size()
    {
        return size;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clear()
    {
        nextWriteOffset = 0;
        index.clear();
        size = 0;
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