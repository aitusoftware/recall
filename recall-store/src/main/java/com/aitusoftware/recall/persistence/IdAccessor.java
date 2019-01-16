package com.aitusoftware.recall.persistence;

@FunctionalInterface
public interface IdAccessor<T>
{
    long getId(T value);
}
