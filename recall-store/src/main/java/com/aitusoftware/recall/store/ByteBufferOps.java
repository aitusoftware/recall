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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Utility class for performing operations on an {@link ByteBuffer}.
 */
public final class ByteBufferOps extends BufferOps<ByteBuffer>
{
    /**
     * {@inheritDoc}
     */
    @Override
    ByteBuffer createFrom(final FileChannel fileChannel, final int offset, final int length)
    {
        final ByteBuffer buffer = ByteBuffer.allocateDirect(length);
        try
        {
            fileChannel.position(offset);
            while (buffer.remaining() != 0)
            {
                fileChannel.read(buffer);
            }
        }
        catch (final IOException e)
        {
            throw new UncheckedIOException(e);
        }
        buffer.flip();
        return buffer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    void storeTo(final FileChannel fileChannel, final ByteBuffer buffer, final int length)
    {
        int lengthRemaining = length;
        buffer.position(0).limit(length);
        while (lengthRemaining != 0)
        {
            try
            {
                lengthRemaining -= fileChannel.write(buffer);
            }
            catch (final IOException e)
            {
                throw new UncheckedIOException(e);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    void writeLong(final ByteBuffer buffer, final int offset, final long value)
    {
        buffer.putLong(offset, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    long readLong(final ByteBuffer buffer, final int offset)
    {
        return buffer.getLong(offset);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    void writeInt(final ByteBuffer buffer, final int offset, final int value)
    {
        buffer.putInt(offset, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    int readInt(final ByteBuffer buffer, final int offset)
    {
        return buffer.getInt(offset);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    void writeByte(final ByteBuffer buffer, final int offset, final byte value)
    {
        buffer.put(offset, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    byte readByte(final ByteBuffer buffer, final int offset)
    {
        return buffer.get(offset);
    }
}
