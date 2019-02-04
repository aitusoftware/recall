/*
 * Copyright 2019 Aitu Software Limited.
 *
 * https://aitusoftware.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.aitusoftware.recall.persistence;

/**
 * Mutable {@link CharSequence}.
 */
public final class AsciiCharSequence implements CharSequence
{
    private final char[] content;
    private int length;

    /**
     * Construct a new instance.
     *
     * @param value the initial value
     */
    public AsciiCharSequence(final String value)
    {
        content = new char[value.length() * 4];
        System.arraycopy(value.toCharArray(), 0, content, 0, value.length());
        length = value.length();
    }

    /**
     * Construct a new instance.
     *
     * @param maxLength maximum length that will be encoded
     */
    public AsciiCharSequence(final int maxLength)
    {
        this.content = new char[maxLength];
    }

    /**
     * Reset the <code>CharSequence</code>.
     */
    public void reset()
    {
        length = 0;
    }

    /**
     * Append a character.
     *
     * @param value value to append
     */
    public void append(final char value)
    {
        content[length++] = value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int length()
    {
        return length;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public char charAt(final int i)
    {
        return content[i];
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CharSequence subSequence(final int i, final int i1)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return new String(content, 0, length);
    }
}