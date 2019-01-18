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

/**
 * Utility class for performing operations on a buffer.
 *
 * @param <T> the type of the buffer
 */
public abstract class BufferOps<T>
{
    /**
     * Write a long to the specified buffer.
     *
     * @param buffer the target buffer
     * @param offset the offset into the buffer
     * @param value  the value to write
     */
    abstract void writeLong(T buffer, int offset, long value);

    /**
     * Read a long from the specified buffer.
     *
     * @param buffer the source buffer
     * @param offset the offset into the buffer
     * @return the value at the specified offset
     */
    abstract long readLong(T buffer, int offset);

    /**
     * Write a byte to the specified buffer.
     *
     * @param buffer the target buffer
     * @param offset the offset into the buffer
     * @param value  the value to write
     */
    abstract void writeByte(T buffer, int offset, byte value);

    /**
     * Read a byte from the specified buffer.
     *
     * @param buffer the source buffer
     * @param offset the offset into the buffer
     * @return the value at the specified offset
     */
    abstract byte readByte(T buffer, int offset);

    /**
     * Copy bytes between buffers.
     *
     * @param source       source buffer
     * @param target       target buffer
     * @param sourceOffset offset in source buffer
     * @param targetOffset offset in target buffer
     * @param length       length of data to be copied
     */
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