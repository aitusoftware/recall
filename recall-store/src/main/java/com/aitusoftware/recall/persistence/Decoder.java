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
package com.aitusoftware.recall.persistence;

/**
 * Deserialiser to be used with a {@see Store}.
 *
 * @param <B> type of the buffer
 * @param <T> type of the instance
 */
@FunctionalInterface
public interface Decoder<B, T>
{
    /**
     * Decodes the value at the specified offset into the supplied container instance.
     *
     * @param buffer    source buffer
     * @param offset    offset into the buffer
     * @param container receiver for the data
     */
    void load(B buffer, int offset, T container);
}
