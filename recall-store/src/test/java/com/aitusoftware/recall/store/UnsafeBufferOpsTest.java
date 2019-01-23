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
package com.aitusoftware.recall.store;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static com.google.common.truth.Truth.assertThat;

class UnsafeBufferOpsTest
{
    @Test
    void shouldCopyBytes()
    {
        final byte[] data = "0123456789ABCDE".getBytes(StandardCharsets.UTF_8);
        final UnsafeBuffer source = new UnsafeBuffer();
        source.wrap(data);
        final UnsafeBuffer target = new UnsafeBuffer(new byte[32]);
        final UnsafeBuffer expected = new UnsafeBuffer(new byte[32]);
        expected.putBytes(7, data);

        final UnsafeBufferOps ops = new UnsafeBufferOps();
        ops.copyBytes(source, target, 0, 7, source.capacity());

        assertThat(Arrays.equals(target.byteArray(), expected.byteArray())).isTrue();
    }
}