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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.function.IntFunction;

/**
 * Implementation of {@link Store} that serialises data to a buffer of type <code>B</code>.
 *
 * @param <B> type of the underlying buffer
 */
public final class BufferStore<B> implements Store<B>
{
    private static final long NOT_IN_MAP = Long.MIN_VALUE;
    private static final int DATA_OFFSET = Header.LENGTH;
    private static final int HEADER_OFFSET = 0;
    private final Long2LongHashMap index = new Long2LongHashMap(NOT_IN_MAP);
    private final int internalRecordLength;
    private final BufferOps<B> bufferOps;
    private final IntFunction<B> bufferFactory;
    private final Header header;
    private int bufferCapacity;
    private B buffer;
    private int nextWriteOffset;
    private int size;

    /**
     * Constructor for the BufferStore.
     *
     * @param maxRecordLength max length of any record
     * @param initialSize     initial number of records that need to be stored
     * @param bufferFactory   provider for the underlying buffer type
     * @param bufferOps       provider of operations on the underlying buffer type
     */
    public BufferStore(
        final int maxRecordLength, final int initialSize,
        final IntFunction<B> bufferFactory,
        final BufferOps<B> bufferOps)
    {
        internalRecordLength = maxRecordLength + Long.BYTES;
        bufferCapacity = internalRecordLength * initialSize;
        this.bufferOps = bufferOps;
        this.bufferFactory = bufferFactory;
        buffer = this.bufferFactory.apply(bufferCapacity + DATA_OFFSET);
        nextWriteOffset = DATA_OFFSET;
        header = new Header();
        header.maxRecordLength(maxRecordLength).version(Version.ONE)
            .storeLength(bufferCapacity).nextWriteOffset(nextWriteOffset);
        header.writeTo(buffer, bufferOps, HEADER_OFFSET);
    }

    private BufferStore(
        final IntFunction<B> bufferFactory, final BufferOps<B> bufferOps,
        final B existingBuffer, final Header header)
    {
        internalRecordLength = header.maxRecordLength() + Long.BYTES;
        bufferCapacity = header.storeLength();
        this.bufferOps = bufferOps;
        this.bufferFactory = bufferFactory;
        buffer = existingBuffer;
        this.nextWriteOffset = header.nextWriteOffset();
        this.header = header;
        final int numberOfRecords = nextWriteOffset / internalRecordLength;
        for (int i = 0; i < numberOfRecords; i++)
        {
            final int entryOffset = (i * internalRecordLength) + DATA_OFFSET;
            final long id = bufferOps.readLong(buffer, entryOffset);
            index.put(id, entryOffset);
        }
    }

    public static <B> BufferStore<B> loadFrom(
        final FileChannel input, final BufferOps<B> bufferOps, final IntFunction<B> bufferFactory)
    {
        final ByteBuffer headerBuffer = ByteBuffer.allocateDirect(Header.LENGTH);
        try
        {
            input.position(0);
            while (headerBuffer.remaining() != 0)
            {
                input.read(headerBuffer);
            }
        }
        catch (final IOException e)
        {
            throw new UncheckedIOException(e);
        }
        headerBuffer.flip();
        final Header header = new Header();
        header.readFrom(headerBuffer, bufferOps.byteOrder());

        final B buffer = bufferOps.createFrom(input, 0, header.storeLength() + Header.LENGTH);
        return new BufferStore<>(bufferFactory, bufferOps, buffer, header);
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
        final long valueId = idAccessor.getId(value);

        if (nextWriteOffset == bufferCapacity + DATA_OFFSET)
        {
            final B expandedBuffer = bufferFactory.apply((bufferCapacity << 1) + Header.LENGTH);
            bufferOps.copyBytes(buffer, expandedBuffer, DATA_OFFSET, DATA_OFFSET, bufferCapacity);
            buffer = expandedBuffer;
            bufferCapacity <<= 1;
            header.storeLength(bufferCapacity).writeTo(buffer, bufferOps, HEADER_OFFSET);
        }

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
        try
        {
            encoder.store(this.buffer, recordWriteOffset, value);
        }
        catch (final IllegalArgumentException e)
        {
            throw new IllegalArgumentException(String.format("Failed to store value with id %d at offset %d",
                valueId, recordWriteOffset), e);
        }
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
    public void writeTo(final FileChannel output)
    {
        header.nextWriteOffset(nextWriteOffset).writeTo(buffer, bufferOps, HEADER_OFFSET);

        bufferOps.storeTo(output, buffer, bufferCapacity + Header.LENGTH);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public float utilisation()
    {
        return (nextWriteOffset - Header.LENGTH) / (float)bufferCapacity;
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
        nextWriteOffset = DATA_OFFSET;
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