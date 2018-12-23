package com.aitusoftware.recall.persistence;

import java.util.Arrays;

public final class AsciiCharSequence implements CharSequence
{
    private final char[] content;
    private int length;

    public AsciiCharSequence(final String value)
    {
        content = new char[value.length()];
        System.arraycopy(value.toCharArray(), 0, content, 0, content.length);
        length = content.length;
    }

    public AsciiCharSequence(final int maxLength)
    {
        this.content = new char[maxLength];
    }

    public void reset()
    {
        length = 0;
    }

    public void append(final char value)
    {
        content[length++] = value;
    }


    @Override
    public int length()
    {
        return length;
    }

    @Override
    public char charAt(final int i)
    {
        return content[i];
    }

    @Override
    public CharSequence subSequence(final int i, final int i1)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString()
    {
        return "AsciiCharSequence{" +
                "content=" + Arrays.toString(content) +
                ", length=" + length +
                '}';
    }
}
