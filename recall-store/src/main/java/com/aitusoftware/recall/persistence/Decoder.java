package com.aitusoftware.recall.persistence;

public interface Decoder<B, T>
{
    void load(B buffer, int offset, T container);
}
