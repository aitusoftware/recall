package com.aitusoftware.recall.sbe;

import org.agrona.sbe.Flyweight;
import org.agrona.sbe.MessageFlyweight;

public final class SbeObjectDescriptor
{
    private final MessageFlyweight messageFlyweight;
    private final Flyweight headerFlyweight;

    public SbeObjectDescriptor(final MessageFlyweight messageFlyweight, final Flyweight headerFlyweight)
    {
        this.messageFlyweight = messageFlyweight;
        this.headerFlyweight = headerFlyweight;
    }


}