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

import java.nio.channels.FileChannel;

/**
 * A storage medium for serialisable data.
 *
 * @param <B> type of the backing buffer that data will be serialised to
 */
public interface Store<B>
{
    /**
     * Attempts to load the value belonging to the specified identifier.
     *
     * @param id        the identifier of the value to retrieve
     * @param decoder   the {@link Decoder} to use to deserialise the data
     * @param container the instance to deserialise data into
     * @param <T>       the type of the object being deserialised
     * @return          indicates whether the identifier was found in the store
     */
    <T> boolean load(long id, Decoder<B, T> decoder, T container);

    /**
     * Attempts to store the a value.
     *
     * @param encoder    the {@link Encoder} to use to serialise the data
     * @param value      the data to serialise
     * @param idAccessor the function to retrieve the identifier of the value
     * @param <T>        the type of the data
     */
    <T> void store(Encoder<B, T> encoder, T value, IdAccessor<T> idAccessor);

    /**
     * Attempts to remove the value belonging to the specified identifier.
     *
     * @param id the identifier of the value to remove
     * @return indicates whether the value was removed
     */
    boolean remove(long id);

    /**
     * Perform a compaction operation on the underlying store.
     * This is implementation dependent.
     */
    void compact();

    /**
     * If storage is provided by an in-memory store, persist it to a more
     * reliable medium (e.g. sync to disk).
     */
    void sync();

    /**
     * Write contents of store to the supplied {@code FileChannel}.
     *
     * @param output the file to write to
     */
    void writeTo(FileChannel output);

    /**
     * Return the current utilisation of the Store capacity.
     *
     * @return current utilisation
     */
    float utilisation();

    /**
     * Return the number of elements in the Store.
     *
     * @return number of elements
     */
    int size();

    /**
     * Clears all entries from the Store.
     */
    void clear();
}