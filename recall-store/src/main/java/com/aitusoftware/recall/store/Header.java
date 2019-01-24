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

final class Header
{
    static final int LENGTH = 4 * Integer.BYTES;
    private static final int STORE_LENGTH_OFFSET = Integer.BYTES;
    private static final int RECORD_LENGTH_OFFSET = 2 * Integer.BYTES;

    private Version version;
    private int storeLength;
    private int maxRecordLength;

    <B> void readFrom(final B input, final BufferOps<B> bufferOps, final int offset)
    {
        version = Version.from(bufferOps.readInt(input, offset));
        storeLength = bufferOps.readInt(input, offset + STORE_LENGTH_OFFSET);
        maxRecordLength = bufferOps.readInt(input, offset + RECORD_LENGTH_OFFSET);
    }

    <B> void writeTo(final B input, final BufferOps<B> bufferOps, final int offset)
    {
        bufferOps.writeInt(input, offset, version.getVersionNumber());
        bufferOps.writeInt(input, offset + STORE_LENGTH_OFFSET, storeLength);
        bufferOps.writeInt(input, offset + RECORD_LENGTH_OFFSET, maxRecordLength);
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
}