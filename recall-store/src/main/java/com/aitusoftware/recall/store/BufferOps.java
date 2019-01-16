package com.aitusoftware.recall.store;

public abstract class BufferOps<T>
{
    abstract void writeLong(T buffer, int offset, long value);

    abstract long readLong(T buffer, int offset);

    abstract void writeByte(T buffer, int offset, byte value);

    abstract byte readByte(T buffer, int offset);

    protected void copyBytes(
        final T source, final T target, final int sourceOffset, final int targetOffset, final int length)
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
