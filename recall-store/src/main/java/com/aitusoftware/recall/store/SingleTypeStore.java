package com.aitusoftware.recall.store;

import com.aitusoftware.recall.persistence.Decoder;
import com.aitusoftware.recall.persistence.Encoder;
import com.aitusoftware.recall.persistence.IdAccessor;

import java.io.OutputStream;

public final class SingleTypeStore<B, T>
{
    private final Store<B> store;
    private final Decoder<B, T> decoder;
    private final Encoder<B, T> encoder;
    private final IdAccessor<T> idAccessor;

    public SingleTypeStore(
        final Store<B> store, final Decoder<B, T> decoder,
        final Encoder<B, T> encoder, final IdAccessor<T> idAccessor)
    {
        this.store = store;
        this.decoder = decoder;
        this.encoder = encoder;
        this.idAccessor = idAccessor;
    }

    public boolean load(final long id, final T container)
    {
        return store.load(id, decoder, container);
    }

    public void store(final T value) throws CapacityExceededException
    {
        store.store(encoder, value, idAccessor);
    }

    public boolean remove(final long id)
    {
        return store.remove(id);
    }

    public void compact()
    {
        store.compact();
    }

    public void sync()
    {
        store.sync();
    }

    public void streamTo(final OutputStream output)
    {
        store.streamTo(output);
    }

    public float utilisation()
    {
        return store.utilisation();
    }

    public void clear()
    {
        store.clear();
    }

    public Store<B> store()
    {
        return store;
    }
}