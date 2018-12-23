package com.aitusoftware.recall.persistence;

public interface Transcoder<B, T>
{
    void load(final B buffer, final T container);
    void store(final B buffer, final T value);
}
