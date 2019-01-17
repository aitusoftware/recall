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