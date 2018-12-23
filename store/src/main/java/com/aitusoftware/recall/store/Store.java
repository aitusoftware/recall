package com.aitusoftware.recall.store;

import com.aitusoftware.recall.persistence.IdAccessor;
import com.aitusoftware.recall.persistence.Transcoder;

import java.io.OutputStream;

public interface Store<B>
{
    <T> boolean load(final long id, final B buffer, final Transcoder<B, T> transcoder, final T container);
    <T> void store(final B buffer, final Transcoder<B, T> transcoder, final T value, final IdAccessor<T> idAccessor);
    boolean remove(final long id);
    void sync();
    void streamTo(final OutputStream output);
}