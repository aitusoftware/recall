package com.aitusoftware.recall.store;

import java.nio.ByteBuffer;

public class ByteBufferOps extends BufferOps<ByteBuffer>
{
    @Override
    void writeLong(final ByteBuffer buffer, final int offset, final long value)
    {
        buffer.putLong(offset, value);
    }

    @Override
    long readLong(final ByteBuffer buffer, final int offset)
    {
        return buffer.getLong(offset);
    }

    @Override
    void writeByte(final ByteBuffer buffer, final int offset, final byte value)
    {
        buffer.put(offset, value);
    }

    @Override
    byte readByte(final ByteBuffer buffer, final int offset)
    {
        return buffer.get(offset);
    }
}
