package com.aitusoftware.recall.benchmark;

import com.aitusoftware.recall.example.Order;
import com.aitusoftware.recall.example.OrderByteBufferTranscoder;
import com.aitusoftware.recall.persistence.IdAccessor;
import com.aitusoftware.recall.store.BufferStore;
import com.aitusoftware.recall.store.ByteBufferOps;
import com.aitusoftware.recall.store.Store;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.nio.ByteBuffer;

@State(Scope.Benchmark)
public class ByteBufferStoreBenchmark
{
    private static final long ORDER_ID = 77L;

    private final Store<ByteBuffer> store = new BufferStore<>(
        200, 64, ByteBuffer::allocateDirect, new ByteBufferOps());
    private final Order order = new Order(ORDER_ID, System.currentTimeMillis(), 9_999,
        Long.MAX_VALUE, System.currentTimeMillis(), 7_777, "FTSE100");
    private final OrderByteBufferTranscoder transcoder = new OrderByteBufferTranscoder();
    private final IdAccessor<Order> idAccessor = new OrderIdAccessor();

    @Setup
    public void setup()
    {
        store.store(transcoder, order, idAccessor);
    }

    @Benchmark
    public long store()
    {
        store.store(transcoder, order, idAccessor);
        return store.size();
    }

    @Benchmark
    public long load()
    {
        store.load(ORDER_ID, transcoder, order);
        return order.getId();
    }

    private static final class OrderIdAccessor implements IdAccessor<Order>
    {
        @Override
        public long getId(final Order value)
        {
            return value.getId();
        }
    }
}
