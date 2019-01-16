package com.aitusoftware.recall.sbe;

import com.aitusoftware.recall.sbe.example.CarDecoder;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SbeMessageBufferEncoderTest
{
    private final SbeMessageBufferEncoder<CarDecoder> encoder = new SbeMessageBufferEncoder<>(10);

    @Test
    void shouldThrowExceptionIfEncodedLengthExceedMaxMessageLength()
    {
        final CarDecoder carDecoder = new CarDecoder();
        carDecoder.wrap(new UnsafeBuffer(new byte[64]), 0, carDecoder.sbeBlockLength(), 0);
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> encoder.store(new UnsafeBuffer(new byte[0]), 0, carDecoder),
            "Unable to encode message of length 49");
    }
}