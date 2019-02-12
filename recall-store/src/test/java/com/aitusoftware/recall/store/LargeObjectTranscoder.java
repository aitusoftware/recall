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

import java.nio.ByteBuffer;

final class LargeObjectTranscoder
    implements Encoder<ByteBuffer, LargeObject>, Decoder<ByteBuffer, LargeObject>,
    IdAccessor<LargeObject>
{
    @Override
    public void load(final ByteBuffer buffer, final int offset, final LargeObject container)
    {
        buffer.mark();
        buffer.limit(buffer.position() + LargeObject.ONE_MEGABYTE).position(offset);
        final long id = buffer.getLong();
        buffer.get(container.getData());
        container.setId(id);
        buffer.reset();
    }

    @Override
    public void store(final ByteBuffer buffer, final int offset, final LargeObject value)
    {
        buffer.mark();
        buffer.limit(buffer.position() + LargeObject.ONE_MEGABYTE).position(offset);
        buffer.putLong(value.getId());
        buffer.put(value.getData());
        buffer.reset();
    }


    @Override
    public long getId(final LargeObject value)
    {
        return value.getId();
    }
}
