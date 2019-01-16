package com.aitusoftware.recall.annotation.example;

import com.aitusoftware.recall.annotation.LookupField;
import com.aitusoftware.recall.annotation.Recall;

@Recall
public interface ExampleObject
{
    long longValue();
    @LookupField
    int intValue();
    char charValue();
    byte byteValue();
    @LookupField
    CharSequence textValue();
}
