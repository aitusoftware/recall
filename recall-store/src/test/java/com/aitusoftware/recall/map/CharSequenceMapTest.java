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
package com.aitusoftware.recall.map;

import static com.google.common.truth.Truth.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

class CharSequenceMapTest
{
    private static final String SEARCH_TERM = "searchTerm";
    private static final long ID = 17L;
    private static final int INITIAL_SIZE = 16;

    private final CharSequenceMap index = new CharSequenceMap(16, INITIAL_SIZE, Long.MIN_VALUE);
    private final List<Long> receivedList = new ArrayList<>();

    @Test
    void shouldStoreSingleValue()
    {
        index.insert(SEARCH_TERM, ID);

        assertSearchResult(index, SEARCH_TERM, ID);
    }

    @Test
    void shouldNotRetrieveUnknownValue()
    {
        index.search(SEARCH_TERM);

        assertThat(receivedList).isEmpty();
    }

    @Test
    void shouldRetrieveMultipleValuesWhenHashesCollide()
    {
        final CharSequenceMap poorIndex = new CharSequenceMap(16, 10, cs -> 7, Long.MIN_VALUE);

        final String otherTerm = "otherTerm";
        final long otherId = 99L;

        poorIndex.insert(SEARCH_TERM, ID);
        poorIndex.insert(otherTerm, otherId);

        assertSearchResult(poorIndex, SEARCH_TERM, ID);

        receivedList.clear();

        assertSearchResult(poorIndex, otherTerm, otherId);
    }

    @Test
    void shouldHaveCorrectIndexAfterResize()
    {
        final int doubleInitialSize = INITIAL_SIZE * 2;
        for (int i = 0; i < doubleInitialSize; i++)
        {
            index.insert("searchTerm_" + i, i);
        }

        for (int i = 0; i < doubleInitialSize; i++)
        {
            receivedList.clear();
            assertSearchResult(index, "searchTerm_" + i, i);
        }
    }

    @Test
    void shouldReplaceExistingValue()
    {
        final long otherId = 42L;
        index.insert(SEARCH_TERM, ID);
        index.insert(SEARCH_TERM, otherId);

        assertSearchResult(index, SEARCH_TERM, otherId);
    }

    @Test
    void shouldWrapBuffer()
    {
        final int initialSize = 20;
        final CharSequenceMap map = new CharSequenceMap(64, initialSize, Long.MIN_VALUE);
        final String prefix = "SYM_";
        for (int i = 0; i < initialSize; i++)
        {
            map.insert(prefix + i, i);
        }

        for (int i = 0; i < initialSize; i++)
        {
            assertThat(map.search(prefix + i)).isEqualTo(i);
        }
    }

    private void assertSearchResult(final CharSequenceMap index, final String searchTerm, final long retrievedId)
    {
        assertThat(index.search(searchTerm)).isEqualTo(retrievedId);
    }
}