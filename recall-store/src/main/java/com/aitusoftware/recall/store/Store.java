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

import com.aitusoftware.recall.persistence.Decoder;
import com.aitusoftware.recall.persistence.Encoder;
import com.aitusoftware.recall.persistence.IdAccessor;

import java.io.OutputStream;

public interface Store<B>
{
    <T> boolean load(long id, Decoder<B, T> decoder, T container);

    <T> void store(Encoder<B, T> encoder, T value, IdAccessor<T> idAccessor)
        throws CapacityExceededException;

    boolean remove(long id);

    void compact();

    void sync();

    void streamTo(OutputStream output);

    float utilisation();

    int size();

    void clear();
}