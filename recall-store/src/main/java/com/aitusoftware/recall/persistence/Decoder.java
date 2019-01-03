package com.aitusoftware.recall.persistence;

public interface Decoder<B, T>
{
    void load(final B buffer, final int offset, final T container);
}
