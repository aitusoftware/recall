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

import java.util.Arrays;

public final class AsciiCharSequence implements CharSequence
{
    private final char[] content;
    private int length;

    public AsciiCharSequence(final String value)
    {
        content = new char[value.length() * 4];
        System.arraycopy(value.toCharArray(), 0, content, 0, value.length());
        length = value.length();
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
