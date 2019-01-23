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


import com.aitusoftware.recall.example.Order;
import com.aitusoftware.recall.example.OrderByteBufferTranscoder;
import org.agrona.collections.LongHashSet;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.function.IntFunction;

import static com.google.common.truth.Truth.assertThat;

class ByteBufferStoreTest
{
    private static final long ID = 17L;
    private static final int MAX_RECORDS = 16;
    private final IntFunction<ByteBuffer> bufferFactory = ByteBuffer::allocate;
    private final ByteBufferOps bufferOps = new ByteBufferOps();
    private final BufferStore<ByteBuffer> store =
        new BufferStore<>(64, MAX_RECORDS, bufferFactory, bufferOps);
    private final OrderByteBufferTranscoder transcoder = new OrderByteBufferTranscoder();

    @Test
    void shouldStoreAndLoad()
    {
        final Order order = Order.of(ID);
        store.store(transcoder, order, order);

        final Order container = Order.of(-1L);
        assertThat(store.load(ID, transcoder, container)).isTrue();

        assertEquality(order, container);
    }

    @Test
    void shouldDelete()
    {
        final Order order = Order.of(ID);
        store.store(transcoder, order, order);

        assertThat(store.remove(ID)).isTrue();

        final Order container = Order.of(-1L);
        assertThat(store.load(ID, transcoder, container)).isFalse();
    }

    @Test
    void shouldIndicateFailedDelete()
    {
        assertThat(store.remove(ID)).isFalse();
    }

    @Test
    void shouldUpdateInPlace()
    {
        final Order order = Order.of(ID);
        store.store(transcoder, order, order);
        final int nextWriteOffset = store.nextWriteOffset();

        final Order updated = new Order(ID, 17L, 37,
            13L, 17L, 35, "Foo");
        store.store(transcoder, updated, updated);

        assertThat(store.nextWriteOffset()).isEqualTo(nextWriteOffset);

        final Order container = Order.of(-1L);
        assertThat(store.load(ID, transcoder, container)).isTrue();

        assertEquality(container, updated);
    }

    @Test
    void shouldStoreAfterRemoval()
    {
        final Order order = Order.of(ID);
        store.store(transcoder, order, order);

        store.remove(ID);

        store.store(transcoder, order, order);

        final Order container = Order.of(-1L);
        assertThat(store.load(ID, transcoder, container)).isTrue();

        assertEquality(container, order);
    }

    @Test
    void shouldGrowIfInitialCapacityExceeded()
    {
        for (int i = 0; i < MAX_RECORDS; i++)
        {
            final Order order = Order.of(i);
            store.store(transcoder, order, order);
        }

        assertThat(store.utilisation() > 0.99).isTrue();

        final Order order = Order.of(MAX_RECORDS);
        store.store(transcoder, order, order);

        assertThat(store.utilisation() < 0.6).isTrue();
        assertThat(store.size()).isEqualTo(MAX_RECORDS + 1);

        for (int i = 0; i <= MAX_RECORDS; i++)
        {
            assertThat(store.load(i, transcoder, Order.of(-1)))
                .named("Loading %d", i).isTrue();
        }
    }

    @Test
    void shouldReportUtilisation()
    {
        assertThat(store.utilisation()).isEqualTo(0f);
        store.store(transcoder, Order.of(1), Order.of(1));
        store.store(transcoder, Order.of(2), Order.of(1));

        assertThat(store.utilisation()).isWithin(0.01f).of(2 / (float)MAX_RECORDS);

        for (int i = 0; i < 6; i++)
        {
            store.store(transcoder, Order.of(3 + i), Order.of(1));
        }

        assertThat(store.utilisation()).isWithin(0.01f).of(0.5f);

        for (int i = 0; i < 8; i++)
        {
            store.store(transcoder, Order.of(13 + i), Order.of(1));
        }

        assertThat(store.utilisation()).isWithin(0.01f).of(1f);
    }

    @Test
    void shouldCompactAfterRemoval()
    {
        for (int i = 0; i < MAX_RECORDS; i++)
        {
            final Order order = Order.of(i);
            store.store(transcoder, order, order);
        }

        int expectedSize = store.size();
        for (int i = 0; i < MAX_RECORDS; i += 2)
        {
            assertThat(store.remove(i)).isTrue();
            assertThat(store.size()).isEqualTo(--expectedSize);
        }

        store.compact();

        for (int i = 0; i < (MAX_RECORDS / 2); i++)
        {
            final Order order = Order.of(i + MAX_RECORDS);
            store.store(transcoder, order, order);
        }

        final Order container = Order.of(-1L);

        for (int i = 1; i < MAX_RECORDS; i += 2)
        {
            assertThat(store.load(i, transcoder, container)).named("Did not find element %d", i).isTrue();
        }
        for (int i = 0; i < (MAX_RECORDS / 2); i++)
        {
            assertThat(store.load(i + MAX_RECORDS, transcoder, container)).isTrue();
        }
    }

    @Test
    void shouldPerformIdealCompaction()
    {
        for (int i = 0; i < 4; i++)
        {
            final Order order = Order.of(i);
            store.store(transcoder, order, order);
        }

        for (int i = 0; i < 4; i += 2)
        {
            store.remove(i);
        }

        store.compact();

        assertThat(store.nextWriteOffset()).isEqualTo(144);
    }

    @Test
    void correctnessTest()
    {
        final long randomSeed = System.nanoTime();
        final Random random = new Random(randomSeed);
        final int maxRecords = 50_000;
        final BufferStore<ByteBuffer> store = new BufferStore<>(128, maxRecords, bufferFactory, bufferOps);
        final LongHashSet createdIds = new LongHashSet();
        final LongHashSet removedIds = new LongHashSet();

        for (int i = 0; i < maxRecords; i++)
        {
            final long id = random.nextLong();
            store.store(transcoder, Order.of(id), ByteBufferStoreTest::idOf);
            createdIds.add(id);
            assertThat(store.size()).isEqualTo(i + 1);
        }

        final LongHashSet.LongIterator iterator = createdIds.iterator();
        for (int i = 0; i < maxRecords / 10; i++)
        {
            final long idToRemove = iterator.nextValue();
            createdIds.remove(idToRemove);
            removedIds.add(idToRemove);
            store.remove(idToRemove);
            if (i % 100 == 0)
            {
                store.compact();
            }
        }
        try
        {
            assertContent(store, createdIds, removedIds);
        }
        catch (final AssertionError e)
        {
            throw new AssertionError("Test failed with random seed " + randomSeed, e);
        }

        store.clear();

        assertThat(store.size()).isEqualTo(0);
        final LongHashSet.LongIterator createdIdIterator = createdIds.iterator();
        final Order container = Order.of(-1L);
        while (createdIdIterator.hasNext())
        {
            assertThat(store.load(createdIdIterator.nextValue(), transcoder, container)).isFalse();
        }
    }

    private void assertContent(
        final BufferStore<ByteBuffer> store, final LongHashSet createdIds,
        final LongHashSet removedIds)
    {
        final Order container = Order.of(99999L);

        for (final long id : createdIds)
        {
            assertThat(store.load(id, transcoder, container)).isTrue();
            assertThat(container.getId()).isEqualTo(id);
            assertThat(container.getInstrumentId()).isEqualTo(37L);
        }

        for (final long id : removedIds)
        {
            assertThat(store.load(id, transcoder, container)).isFalse();
        }
    }

    private static long idOf(final Order order)
    {
        return order.getId();
    }

    private static void assertEquality(final Order actual, final Order expected)
    {
        assertThat(actual.getId()).isEqualTo(expected.getId());
        assertCharSequenceEquality(actual.getSymbol(), expected.getSymbol());
        assertThat(actual.getCreatedEpochSeconds()).isEqualTo(expected.getCreatedEpochSeconds());
    }

    private static void assertCharSequenceEquality(final CharSequence actual, final CharSequence expected)
    {
        assertThat(actual.length()).isEqualTo(expected.length());
        for (int i = 0; i < actual.length(); i++)
        {
            assertThat(actual.charAt(i)).isEqualTo(expected.charAt(i));
        }
    }
}