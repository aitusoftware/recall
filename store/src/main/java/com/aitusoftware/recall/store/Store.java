package com.aitusoftware.recall.store;

import com.aitusoftware.recall.persistence.IdAccessor;
import com.aitusoftware.recall.persistence.Transcoder;

public interface Store<B>
{
    <T> boolean load(final long id, final B buffer, final Transcoder<B, T> transcoder, final T container);
    <T> void store(final B buffer, final Transcoder<B, T> transcoder, final T value, final IdAccessor<T> idAccessor);
}