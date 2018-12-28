package com.aitusoftware.recall.example;

import com.aitusoftware.recall.persistence.AsciiCharSequence;
import com.aitusoftware.recall.persistence.Decoder;
import com.aitusoftware.recall.persistence.Encoder;

import java.nio.ByteBuffer;

public class OrderByteBufferTranscoder implements Encoder<ByteBuffer, Order>, Decoder<ByteBuffer, Order>
{
    @Override
    public void load(final ByteBuffer buffer, final Order container)
    {
        container.setId(buffer.getLong());
        container.setInstrumentId(buffer.getLong());
        container.setCreatedEpochSeconds(buffer.getLong());
        container.setCreatedNanos(buffer.getInt());
        container.setExecutedAtEpochSeconds(buffer.getLong());
        container.setExecutedAtNanos(buffer.getInt());
        int charCount = buffer.getInt();
        AsciiCharSequence symbolCharSequence = container.getSymbolCharSequence();
        symbolCharSequence.reset();
        for (int i = 0; i < charCount; i++)
        {
            symbolCharSequence.append((char) buffer.get());
        }
    }

    @Override
    public void store(final ByteBuffer buffer, final Order value)
    {
        buffer.putLong(value.getId());
        buffer.putLong(value.getInstrumentId());
        buffer.putLong(value.getCreatedEpochSeconds());
        buffer.putInt(value.getCreatedNanos());
        buffer.putLong(value.getExecutedAtEpochSeconds());
        buffer.putInt(value.getExecutedAtNanos());
        final CharSequence symbol = value.getSymbol();
        buffer.putInt(symbol.length());
        for (int i = 0; i < symbol.length(); i++) {
            buffer.put((byte) symbol.charAt(i));
        }
    }
}
