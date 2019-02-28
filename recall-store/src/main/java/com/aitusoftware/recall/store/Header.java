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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

final class Header
{
    static final int LENGTH = 4 * Integer.BYTES;
    private static final int VERSION_OFFSET = 0;
    private static final int STORE_LENGTH_OFFSET = Integer.BYTES;
    private static final int RECORD_LENGTH_OFFSET = 2 * Integer.BYTES;
    private static final int WRITE_OFFSET_OFFSET = 3 * Integer.BYTES;
    private static final ByteOrder STORAGE_ORDER = ByteOrder.LITTLE_ENDIAN;

    private Version version;
    private int storeLength;
    private int maxRecordLength;
    private int nextWriteOffset;

    void readFrom(final ByteBuffer headerBuffer)
    {
        version = Version.from(headerBuffer.order(STORAGE_ORDER).getInt(VERSION_OFFSET));
        storeLength = headerBuffer.order(STORAGE_ORDER).getInt(STORE_LENGTH_OFFSET);
        maxRecordLength = headerBuffer.order(STORAGE_ORDER).getInt(RECORD_LENGTH_OFFSET);
        nextWriteOffset = headerBuffer.order(STORAGE_ORDER).getInt(WRITE_OFFSET_OFFSET);
    }

    <B> void writeTo(final B input, final BufferOps<B> bufferOps, final int offset)
    {
        final ByteOrder bufferOrder = bufferOps.byteOrder();
        bufferOps.writeInt(input, offset + VERSION_OFFSET, littleEndian(version.getVersionNumber(), bufferOrder));
        bufferOps.writeInt(input, offset + STORE_LENGTH_OFFSET, littleEndian(storeLength, bufferOrder));
        bufferOps.writeInt(input, offset + RECORD_LENGTH_OFFSET, littleEndian(maxRecordLength, bufferOrder));
        bufferOps.writeInt(input, offset + WRITE_OFFSET_OFFSET, littleEndian(nextWriteOffset, bufferOrder));
    }

    Version version()
    {
        return version;
    }

    int storeLength()
    {
        return storeLength;
    }

    int maxRecordLength()
    {
        return maxRecordLength;
    }

    int nextWriteOffset()
    {
        return nextWriteOffset;
    }

    Header version(final Version version)
    {
        this.version = version;
        return this;
    }

    Header storeLength(final int storeLength)
    {
        this.storeLength = storeLength;
        return this;
    }

    Header maxRecordLength(final int maxRecordLength)
    {
        this.maxRecordLength = maxRecordLength;
        return this;
    }

    Header nextWriteOffset(final int nextWriteOffset)
    {
        this.nextWriteOffset = nextWriteOffset;
        return this;
    }

    private static int littleEndian(final int value, final ByteOrder byteOrder)
    {
        return byteOrder == ByteOrder.LITTLE_ENDIAN ? value : Integer.reverseBytes(value);
    }
}