package com.aitusoftware.recall.persistence;

public interface Encoder<B, T>
{
    void store(final B buffer, final int offset, final T value);
}
