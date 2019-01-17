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

import com.aitusoftware.recall.persistence.AsciiCharSequence;
import com.aitusoftware.recall.persistence.IdAccessor;

public final class Order implements IdAccessor<Order>
{
    private long id;
    private long createdEpochSeconds;
    private int createdNanos;
    private long instrumentId;
    private long executedAtEpochSeconds;
    private int executedAtNanos;
    private AsciiCharSequence symbol;

    public Order(
        final long id, final long createdEpochSeconds, final int createdNanos,
        final long instrumentId, final long executedAtEpochSeconds,
        final int executedAtNanos, final String symbol)
    {
        this.id = id;
        this.createdEpochSeconds = createdEpochSeconds;
        this.createdNanos = createdNanos;
        this.instrumentId = instrumentId;
        this.executedAtEpochSeconds = executedAtEpochSeconds;
        this.executedAtNanos = executedAtNanos;
        this.symbol = new AsciiCharSequence(symbol);
    }

    public static Order of(final long id)
    {
        return new Order(id, System.currentTimeMillis() / 1000,
            (int)System.currentTimeMillis() % 1000, 37L,
            0L, 0, "SYM_" + id);
    }

    public long getId()
    {
        return id;
    }

    public void setId(final long id)
    {
        this.id = id;
    }

    public long getCreatedEpochSeconds()
    {
        return createdEpochSeconds;
    }

    public void setCreatedEpochSeconds(final long createdEpochSeconds)
    {
        this.createdEpochSeconds = createdEpochSeconds;
    }

    public int getCreatedNanos()
    {
        return createdNanos;
    }

    public void setCreatedNanos(final int createdNanos)
    {
        this.createdNanos = createdNanos;
    }

    public long getInstrumentId()
    {
        return instrumentId;
    }

    public void setInstrumentId(final long instrumentId)
    {
        this.instrumentId = instrumentId;
    }

    public long getExecutedAtEpochSeconds()
    {
        return executedAtEpochSeconds;
    }

    public void setExecutedAtEpochSeconds(final long executedAtEpochSeconds)
    {
        this.executedAtEpochSeconds = executedAtEpochSeconds;
    }

    public int getExecutedAtNanos()
    {
        return executedAtNanos;
    }

    public void setExecutedAtNanos(final int executedAtNanos)
    {
        this.executedAtNanos = executedAtNanos;
    }

    public CharSequence getSymbol()
    {
        return symbol;
    }

    public AsciiCharSequence getSymbolCharSequence()
    {
        return symbol;
    }

    @Override
    public long getId(final Order value)
    {
        return value.getId();
    }
}