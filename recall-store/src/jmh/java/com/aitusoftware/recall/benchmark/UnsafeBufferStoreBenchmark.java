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
package com.aitusoftware.recall.benchmark;

import com.aitusoftware.recall.example.Order;
import com.aitusoftware.recall.example.OrderUnsafeBufferTranscoder;
import com.aitusoftware.recall.persistence.IdAccessor;
import com.aitusoftware.recall.store.BufferStore;
import com.aitusoftware.recall.store.Store;
import com.aitusoftware.recall.store.UnsafeBufferOps;
import org.agrona.concurrent.UnsafeBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

@State(Scope.Benchmark)
public class UnsafeBufferStoreBenchmark
{
    private static final long ORDER_ID = 77L;

    private final Store<UnsafeBuffer> store = new BufferStore<>(
        200, 64,
        len -> new UnsafeBuffer(new byte[len]), new UnsafeBufferOps());
    private final Order order = new Order(ORDER_ID, System.currentTimeMillis(), 9_999,
        Long.MAX_VALUE, System.currentTimeMillis(), 7_777, "FTSE100");
    private final OrderUnsafeBufferTranscoder transcoder = new OrderUnsafeBufferTranscoder();
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
