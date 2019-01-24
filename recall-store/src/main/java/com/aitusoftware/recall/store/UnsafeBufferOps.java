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

import org.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

/**
 * Utility class for performing operations on an {@link UnsafeBuffer}.
 */
public final class UnsafeBufferOps extends BufferOps<UnsafeBuffer>
{
    /**
     * {@inheritDoc}
     */
    @Override
    UnsafeBuffer createFrom(final FileChannel fileChannel, final int offset, final int length)
    {
        final ByteBuffer content = ByteBuffer.allocateDirect(length);
        try
        {
            fileChannel.position(0);
            while (content.remaining() != 0)
            {
                fileChannel.read(content);
            }
        }
        catch (final IOException e)
        {
            throw new UncheckedIOException(e);
        }
        content.clear();
        return new UnsafeBuffer(content);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    void storeTo(final FileChannel fileChannel, final UnsafeBuffer buffer, final int length)
    {
        final ByteBuffer tmp = ByteBuffer.allocateDirect(4096);
        int lengthRemaining = length;
        int offset = 0;
        try
        {
            fileChannel.position(0);
            while (lengthRemaining != 0)
            {
                tmp.clear();
                final int copyLength = Math.min(tmp.capacity(), lengthRemaining);
                buffer.getBytes(offset, tmp, 0, copyLength);
                tmp.position(0).limit(copyLength);
                while (tmp.remaining() != 0)
                {
                    fileChannel.write(tmp);
                }
                lengthRemaining -= copyLength;
                offset += copyLength;
            }
        }
        catch (final IOException e)
        {
            throw new UncheckedIOException(e);
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    void writeLong(final UnsafeBuffer buffer, final int offset, final long value)
    {
        buffer.putLong(offset, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    long readLong(final UnsafeBuffer buffer, final int offset)
    {
        return buffer.getLong(offset);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    void writeInt(final UnsafeBuffer buffer, final int offset, final int value)
    {
        buffer.putInt(offset, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    int readInt(final UnsafeBuffer buffer, final int offset)
    {
        return buffer.getInt(offset);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    void writeByte(final UnsafeBuffer buffer, final int offset, final byte value)
    {
        buffer.putByte(offset, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    byte readByte(final UnsafeBuffer buffer, final int offset)
    {
        return buffer.getByte(offset);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void copyBytes(
        final UnsafeBuffer source, final UnsafeBuffer target, final int sourceOffset,
        final int targetOffset, final int length)
    {
        target.putBytes(targetOffset, source, sourceOffset, length);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    ByteOrder byteOrder()
    {
        return ByteOrder.LITTLE_ENDIAN;
    }
}