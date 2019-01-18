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

import com.aitusoftware.recall.persistence.Encoder;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.sbe.MessageDecoderFlyweight;

/**
 * Encoder for SBE-encoded messages.
 *
 * @param <T> the type of the message
 */
public final class SbeMessageBufferEncoder<T extends MessageDecoderFlyweight> implements Encoder<UnsafeBuffer, T>
{
    private final int maxMessageLength;

    /**
     * Maximum length of any message that will be stored.
     *
     * @param maxMessageLength max message length, in bytes
     */
    public SbeMessageBufferEncoder(final int maxMessageLength)
    {
        this.maxMessageLength = maxMessageLength;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void store(final UnsafeBuffer buffer, final int offset, final T value)
    {
        final int encodedLength = value.encodedLength();
        if (encodedLength > maxMessageLength)
        {
            throw new IllegalArgumentException("Unable to encode message of length " + encodedLength);
        }
        buffer.putBytes(offset, value.buffer(), value.offset(), encodedLength);
    }
}
