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
package com.aitusoftware.recall.example;

import com.aitusoftware.recall.map.CharSequenceMap;
import com.aitusoftware.recall.store.BufferStore;
import com.aitusoftware.recall.store.ByteBufferOps;
import com.aitusoftware.recall.store.SingleTypeStore;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class CharSequenceMapExample
{
    private static final int MAX_RECORD_LENGTH = 128;
    private static final int MAX_KEY_LENGTH = 64;
    private static final int INITIAL_SIZE = 20;

    private final OrderByteBufferTranscoder transcoder =
        new OrderByteBufferTranscoder();
    private final SingleTypeStore<ByteBuffer, Order> store =
        new SingleTypeStore<>(
        new BufferStore<>(MAX_RECORD_LENGTH, INITIAL_SIZE,
        ByteBuffer::allocateDirect, new ByteBufferOps()),
        transcoder, transcoder, Order::getId);
    private final CharSequenceMap orderBySymbol =
        new CharSequenceMap(MAX_KEY_LENGTH, INITIAL_SIZE);

    private void execute()
    {
        final String[] symbols = new String[INITIAL_SIZE];
        for (int i = 0; i < INITIAL_SIZE; i++)
        {
            final Order order = Order.of(i);

            store.store(order);
            orderBySymbol.insert(order.getSymbol(), order.getId());
            symbols[i] = order.getSymbol().toString();
        }

        final Order container = Order.of(-1L);
        final AtomicInteger matchCount = new AtomicInteger();
        for (int i = 0; i < INITIAL_SIZE; i++)
        {
            final String searchTerm = symbols[i];
            orderBySymbol.search(searchTerm, id ->
            {
                store.load(id, container);
                matchCount.incrementAndGet();
                System.out.printf("Order with symbol %s has id %d%n", searchTerm, id);
            });
        }

        if (matchCount.get() != INITIAL_SIZE)
        {
            throw new IllegalStateException();
        }
    }

    public static void main(final String[] args)
    {
        new CharSequenceMapExample().execute();
    }
}
