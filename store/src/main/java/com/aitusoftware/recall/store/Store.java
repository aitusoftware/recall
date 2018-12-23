package com.aitusoftware.recall.store;

import com.aitusoftware.recall.persistence.IdAccessor;
import com.aitusoftware.recall.persistence.Transcoder;

import java.io.OutputStream;

public interface Store<B>
{
    <T> boolean load(final long id, final Transcoder<B, T> transcoder, final T container);
    <T> void store(final Transcoder<B, T> transcoder, final T value, final IdAccessor<T> idAccessor) throws CapacityExceededException;
    boolean remove(final long id);
    void compact();
    void grow();
    void sync();
    void streamTo(final OutputStream output);
    float utilisation();
}