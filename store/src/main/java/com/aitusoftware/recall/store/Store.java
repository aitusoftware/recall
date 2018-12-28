package com.aitusoftware.recall.store;

import com.aitusoftware.recall.persistence.Decoder;
import com.aitusoftware.recall.persistence.Encoder;
import com.aitusoftware.recall.persistence.IdAccessor;

import java.io.OutputStream;

public interface Store<B>
{
    <T> boolean load(final long id, final Decoder<B, T> decoder, final T container);
    <T> void store(final Encoder<B, T> encoder, final T value, final IdAccessor<T> idAccessor) throws CapacityExceededException;
    boolean remove(final long id);
    void compact();
    void grow();
    void sync();
    void streamTo(final OutputStream output);
    float utilisation();
}