package com.aitusoftware.recall.sbe;


import com.aitusoftware.recall.persistence.Decoder;
import com.aitusoftware.recall.persistence.Encoder;
import com.aitusoftware.recall.persistence.IdAccessor;
import com.aitusoftware.recall.sbe.example.*;
import com.aitusoftware.recall.store.BufferStore;
import com.aitusoftware.recall.store.UnsafeBufferOps;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;

class SbeObjectStoreTest
{
    private static final MessageHeaderEncoder HEADER_ENCODER = new MessageHeaderEncoder();
    private static final long ID = 37L;
    private static final String ACTIVATION_CODE = "ACTIVATION_CODE";
    private static final BooleanType TRUE = BooleanType.T;
    private static final Model CODE = Model.A;
    private static final String MANUFACTURER = "Mitsubishi";
    private static final int MODEL_YEAR = 1979;
    private static final String MODEL = "Mirage";
    private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();

    @Test
    void shouldStoreAndRetrieve()
    {
        final CarEncoder encoder = new CarEncoder().wrapAndApplyHeader(buffer, 0, HEADER_ENCODER);
        encoder.id(ID).available(TRUE).code(CODE)
                .modelYear(MODEL_YEAR)
                .manufacturer(MANUFACTURER)
                .model(MODEL)
                .activationCode(ACTIVATION_CODE)
                .engine().boosterEnabled(TRUE);
        final BufferStore<UnsafeBuffer> bufferStore = new BufferStore<>(encoder.encodedLength(), 32,
                len -> new UnsafeBuffer(new byte[len]), new UnsafeBufferOps());
        final CarDecoder decoder = new CarDecoder().wrap(buffer, MessageHeaderEncoder.ENCODED_LENGTH, encoder.encodedLength(), encoder.sbeSchemaVersion());
        assertThat(decoder.id()).isEqualTo(ID);

        bufferStore.store(new CarBufferEncoder(), decoder, new CarIdAccessor());

        final CarDecoder loaded = new CarDecoder();
        assertThat(bufferStore.load(ID, new CarBufferDecoder(), loaded)).isTrue();

        assertThat(loaded.id()).isEqualTo(ID);
        assertThat(loaded.available()).isEqualTo(TRUE);
        assertThat(loaded.code()).isEqualTo(CODE);
        assertThat(loaded.modelYear()).isEqualTo(MODEL_YEAR);
        assertThat(loaded.engine().boosterEnabled()).isEqualTo(TRUE);
        assertThat(loaded.manufacturer()).isEqualTo(MANUFACTURER);
        assertThat(loaded.model()).isEqualTo(MODEL);
        assertThat(loaded.activationCode()).isEqualTo(ACTIVATION_CODE);
    }

    private static final class CarBufferDecoder implements Decoder<UnsafeBuffer, CarDecoder>
    {
        @Override
        public void load(final UnsafeBuffer buffer, final int offset, final CarDecoder container)
        {
            container.wrap(buffer, offset, container.sbeBlockLength(),
                    container.sbeSchemaVersion());
        }

    }

    private static final class CarBufferEncoder implements Encoder<UnsafeBuffer, CarDecoder>
    {
        @Override
        public void store(final UnsafeBuffer buffer, final int offset, final CarDecoder value)
        {
            buffer.putBytes(offset, value.buffer(), value.offset(), value.encodedLength());
        }
    }

    private static final class CarIdAccessor implements IdAccessor<CarDecoder>
    {
        @Override
        public long getId(final CarDecoder value)
        {
            return value.id();
        }
    }
}