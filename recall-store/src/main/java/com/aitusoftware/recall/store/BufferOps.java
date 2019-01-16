package com.aitusoftware.recall.store;

public abstract class BufferOps<T>
{
    abstract void writeLong(final T buffer, final int offset, final long value);

    abstract long readLong(final T buffer, final int offset);

    abstract void writeByte(final T buffer, final int offset, final byte value);

    abstract byte readByte(final T buffer, final int offset);

    void copyBytes(final T source, final T target,
                   final int sourceOffset, final int targetOffset, final int length)
    {
        final int eightByteSegments = length / 8;
        final int trailingBytes = length & 7;
        for (int j = 0; j < eightByteSegments; j += Long.BYTES)
        {
            writeLong(target, targetOffset + j, readLong(source, sourceOffset + j));
        }

        for (int j = 0; j < trailingBytes; j++)
        {
            writeByte(target, targetOffset + j, readByte(source, sourceOffset + j));
        }
    }
}
