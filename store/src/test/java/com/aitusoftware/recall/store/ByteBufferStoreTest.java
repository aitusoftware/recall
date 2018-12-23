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
        store.load(ID, null, transcoder, container);

        assertThat(order.getId()).isEqualTo(container.getId());
        assertThat(order.getSymbol().toString()).isEqualTo(container.getSymbol().toString());
        assertThat(order.getCreatedEpochSeconds()).isEqualTo(container.getCreatedEpochSeconds());
    }
}