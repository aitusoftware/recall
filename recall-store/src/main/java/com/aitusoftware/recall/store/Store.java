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