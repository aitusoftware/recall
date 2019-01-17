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

public final class ByteBufferOps extends BufferOps<ByteBuffer>
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
