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

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static com.google.common.truth.Truth.assertThat;

class ByteBufferOpsTest
{
    @Test
    void shouldCopyBytes()
    {
        final byte[] data = "0123456789ABCDE".getBytes(StandardCharsets.UTF_8);
        final ByteBuffer source = ByteBuffer.wrap(data);
        final ByteBuffer target = ByteBuffer.allocate(32);
        final ByteBuffer expected = ByteBuffer.allocate(32);
        expected.position(7);
        expected.put(data);

        final ByteBufferOps ops = new ByteBufferOps();
        ops.copyBytes(source, target, 0, 7, source.capacity());

        assertThat(Arrays.equals(target.array(), expected.array())).isTrue();
    }
}