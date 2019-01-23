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

/**
 * A store that wraps a {@link Decoder}, {@link Encoder}, and {@link IdAccessor} to
 * provide storage for a single type.
 *
 * @param <B> the type of the underlying buffer
 * @param <T> the type that will be stored
 */
public final class SingleTypeStore<B, T>
{
    private final Store<B> store;
    private final Decoder<B, T> decoder;
    private final Encoder<B, T> encoder;
    private final IdAccessor<T> idAccessor;

    /**
     * Constructor for the store.
     *
     * @param store      the underlying {@link Store}
     * @param decoder    the decoder for deserialising
     * @param encoder    the encode for serialising
     * @param idAccessor the accessor for retrieving the type's ID
     */
    public SingleTypeStore(
        final Store<B> store, final Decoder<B, T> decoder,
        final Encoder<B, T> encoder, final IdAccessor<T> idAccessor)
    {
        this.store = store;
        this.decoder = decoder;
        this.encoder = encoder;
        this.idAccessor = idAccessor;
    }

    /**
     * Loads an entry into the specified container.
     *
     * @param id        id to retrieve
     * @param container container to populate with data
     * @return indicates whether the ID was found
     */
    public boolean load(final long id, final T container)
    {
        return store.load(id, decoder, container);
    }

    /**
     * Stores an entry.
     *
     * @param value the value to be stored
     */
    public void store(final T value)
    {
        store.store(encoder, value, idAccessor);
    }

    /**
     * Removes an entry.
     *
     * @param id id to remove
     * @return indicates whether an entry was removed
     */
    public boolean remove(final long id)
    {
        return store.remove(id);
    }

    /**
     * Delegates to the underlying {@link Store}.
     */
    public void compact()
    {
        store.compact();
    }

    /**
     * Delegates to the underlying {@link Store}.
     */
    public void sync()
    {
        store.sync();
    }

    /**
     * Delegates to the underlying {@link Store}.
     *
     * @param output the target stream
     */
    public void streamTo(final OutputStream output)
    {
        store.streamTo(output);
    }

    /**
     * Delegates to the underlying {@link Store}.
     *
     * @return store utilisation
     */
    public float utilisation()
    {
        return store.utilisation();
    }

    /**
     * Delegates to the underlying {@link Store}.
     */
    public void clear()
    {
        store.clear();
    }

    /**
     * Retrieve the underlying {@link Store}.
     * @return the underlying {@link Store}
     */
    public Store<B> store()
    {
        return store;
    }
}