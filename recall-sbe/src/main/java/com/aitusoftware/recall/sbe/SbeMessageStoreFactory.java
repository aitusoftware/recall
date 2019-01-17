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
package com.aitusoftware.recall.sbe;

import com.aitusoftware.recall.persistence.IdAccessor;
import com.aitusoftware.recall.store.BufferStore;
import com.aitusoftware.recall.store.SingleTypeStore;
import com.aitusoftware.recall.store.UnsafeBufferOps;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.sbe.MessageDecoderFlyweight;

import java.util.function.IntFunction;

public final class SbeMessageStoreFactory
{
    private SbeMessageStoreFactory()
    {
    }

    public static <T extends MessageDecoderFlyweight> SingleTypeStore<UnsafeBuffer, T> forSbeMessage(
        final T decoderFlyweight,
        final int maxMessageLength,
        final int maxRecords,
        final IntFunction<UnsafeBuffer> bufferFactory,
        final IdAccessor<T> idAccessor)
    {
        final BufferStore<UnsafeBuffer> store = new BufferStore<>(
            maxMessageLength, maxRecords, bufferFactory, new UnsafeBufferOps());

        return new SingleTypeStore<>(store, new SbeMessageBufferDecoder<>(),
            new SbeMessageBufferEncoder<>(maxMessageLength), idAccessor);
    }
}