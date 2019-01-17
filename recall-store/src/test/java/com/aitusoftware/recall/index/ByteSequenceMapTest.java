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
package com.aitusoftware.recall.index;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;

class ByteSequenceMapTest
{
    private static final ByteBuffer SEARCH_TERM = toBuffer("searchTerm");
    private static final long ID = 17L;
    private static final int INITIAL_SIZE = 16;
    private final ByteSequenceMap index = new ByteSequenceMap(16, INITIAL_SIZE);
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
        index.search(SEARCH_TERM, this::onReceivedId);

        assertThat(receivedList).isEmpty();
    }

    @Test
    void shouldRetrieveMultipleValuesWhenHashesCollide()
    {
        final ByteSequenceMap poorIndex = new ByteSequenceMap(16, 10, cs -> 7);

        final ByteBuffer otherTerm = toBuffer("otherTerm");
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
            index.insert(toBuffer("searchTerm_" + i), i);
        }

        for (int i = 0; i < doubleInitialSize; i++)
        {
            receivedList.clear();
            assertSearchResult(index, toBuffer("searchTerm_" + i), i);
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

    private void assertSearchResult(final ByteSequenceMap index, final ByteBuffer searchTerm, final long retrievedId)
    {
        index.search(searchTerm, this::onReceivedId);
        assertThat(receivedList).named("Expected value %s for term %s", retrievedId, searchTerm)
                .containsExactly(retrievedId);
    }

    private void onReceivedId(final long id)
    {
        receivedList.add(id);
    }

    private static ByteBuffer toBuffer(final String term)
    {
        return ByteBuffer.wrap(term.getBytes(StandardCharsets.UTF_8));
    }
}