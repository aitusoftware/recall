package com.aitusoftware.recall.persistence;

public interface Encoder<B, T>
{
    void store(B buffer, T value);
}
