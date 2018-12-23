package com.aitusoftware.recall.store;


import com.aitusoftware.recall.example.Order;
import com.aitusoftware.recall.example.OrderByteBufferTranscoder;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;

class ByteBufferStoreTest
{
    private static final long ID = 17L;
    private final ByteBufferStore store = new ByteBufferStore(64, 16);
    private final OrderByteBufferTranscoder transcoder = new OrderByteBufferTranscoder();

    @Test
    void shouldStoreAndLoad()
    {
        final Order order = Order.of(ID);
        store.store(null, transcoder, order, order);

        final Order container = Order.of(-1L);
        assertThat(store.load(ID, null, transcoder, container)).isTrue();

        assertEquality(order, container);
    }

    @Test
    void shouldDelete()
    {
        final Order order = Order.of(ID);
        store.store(null, transcoder, order, order);

        assertThat(store.remove(ID)).isTrue();

        final Order container = Order.of(-1L);
        assertThat(store.load(ID, null, transcoder, container)).isFalse();
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
        store.store(null, transcoder, order, order);
        final int nextWriteOffset = store.getNextWriteOffset();

        final Order updated = new Order(ID, 17L, 37,
                13L, 17L, 35, "Foo");
        store.store(null, transcoder, updated, updated);

        assertThat(store.getNextWriteOffset()).isEqualTo(nextWriteOffset);

        final Order container = Order.of(-1L);
        assertThat(store.load(ID, null, transcoder, container)).isTrue();

        assertEquality(container, updated);
    }

    @Test
    void shouldStoreAfterRemoval()
    {
        final Order order = Order.of(ID);
        store.store(null, transcoder, order, order);

        store.remove(ID);

        store.store(null, transcoder, order, order);

        final Order container = Order.of(-1L);
        assertThat(store.load(ID, null, transcoder, container)).isTrue();

        assertEquality(container, order);
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