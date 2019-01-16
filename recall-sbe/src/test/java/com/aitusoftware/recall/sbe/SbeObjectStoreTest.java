package com.aitusoftware.recall.sbe;


import com.aitusoftware.recall.persistence.IdAccessor;
import com.aitusoftware.recall.sbe.example.*;
import com.aitusoftware.recall.store.BufferStore;
import com.aitusoftware.recall.store.SingleTypeStore;
import com.aitusoftware.recall.store.UnsafeBufferOps;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.IntFunction;

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
    private static final int MAX_RECORD_LENGTH = 256;
    private static final int MAX_RECORDS = 2000;
    private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
    private final BufferStore<UnsafeBuffer> bufferStore = new BufferStore<>(MAX_RECORD_LENGTH, MAX_RECORDS,
            bufferFactory(), new UnsafeBufferOps());
    private final SbeMessageBufferEncoder<CarDecoder> recallEncoder = new SbeMessageBufferEncoder<>(MAX_RECORD_LENGTH);
    private final CarIdAccessor idAccessor = new CarIdAccessor();
    private final SbeMessageBufferDecoder<CarDecoder> recallDecoder = new SbeMessageBufferDecoder<>();

    @Test
    void shouldConstructStoreForType()
    {
        final SingleTypeStore<UnsafeBuffer, CarDecoder> store =
                SbeMessageStoreFactory.forSbeMessage(new CarDecoder(), MAX_RECORD_LENGTH, MAX_RECORDS,
                        bufferFactory(), new CarIdAccessor());

        final CarEncoder encoder = new CarEncoder().wrapAndApplyHeader(buffer, 0, HEADER_ENCODER);
        encoder.id(ID).available(TRUE).code(CODE)
                .modelYear(MODEL_YEAR)
                .manufacturer(MANUFACTURER)
                .model(MODEL)
                .activationCode(ACTIVATION_CODE)
                .engine().boosterEnabled(TRUE);

        final CarDecoder decoder = new CarDecoder().wrap(buffer, MessageHeaderEncoder.ENCODED_LENGTH, encoder.encodedLength(), encoder.sbeSchemaVersion());
        assertThat(decoder.id()).isEqualTo(ID);

        store.store(decoder);

        final CarDecoder loaded = new CarDecoder();
        assertThat(store.load(ID, loaded)).isTrue();
    }

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

        final CarDecoder decoder = new CarDecoder().wrap(buffer, MessageHeaderEncoder.ENCODED_LENGTH, encoder.encodedLength(), encoder.sbeSchemaVersion());
        assertThat(decoder.id()).isEqualTo(ID);

        bufferStore.store(recallEncoder, decoder, idAccessor);

        final CarDecoder loaded = new CarDecoder();
        assertThat(bufferStore.load(ID, recallDecoder, loaded)).isTrue();

        assertThat(loaded.id()).isEqualTo(ID);
        assertThat(loaded.available()).isEqualTo(TRUE);
        assertThat(loaded.code()).isEqualTo(CODE);
        assertThat(loaded.modelYear()).isEqualTo(MODEL_YEAR);
        assertThat(loaded.engine().boosterEnabled()).isEqualTo(TRUE);
        assertThat(loaded.manufacturer()).isEqualTo(MANUFACTURER);
        assertThat(loaded.model()).isEqualTo(MODEL);
        assertThat(loaded.activationCode()).isEqualTo(ACTIVATION_CODE);
    }

    @Test
    void shouldStoreAndRetrieveMultipleRecords()
    {
        final Random random = ThreadLocalRandom.current();
        final long[] ids = new long[MAX_RECORDS];
        for (int i = 0; i < MAX_RECORDS; i++)
        {
            final long id = random.nextLong();
            ids[i] = id;
            final CarEncoder encoder = new CarEncoder().wrapAndApplyHeader(buffer, 0, HEADER_ENCODER);
            encoder.id(id).available(TRUE).code(CODE)
                    .modelYear((int) (id * 17))
                    .manufacturer(MANUFACTURER)
                    .model(MODEL)
                    .activationCode(ACTIVATION_CODE)
                    .engine().boosterEnabled(TRUE);

            final CarDecoder decoder = new CarDecoder().wrap(buffer, MessageHeaderEncoder.ENCODED_LENGTH, encoder.encodedLength(), encoder.sbeSchemaVersion());
            bufferStore.store(recallEncoder, decoder, idAccessor);
        }

        for (int i = 0; i < MAX_RECORDS; i++)
        {
            final CarDecoder loaded = new CarDecoder();
            final long id = ids[i];
            assertThat(bufferStore.load(id, recallDecoder, loaded)).isTrue();
            assertThat(id).isEqualTo(loaded.id());
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

    private static IntFunction<UnsafeBuffer> bufferFactory()
    {
        return len -> new UnsafeBuffer(new byte[len]);
    }
}