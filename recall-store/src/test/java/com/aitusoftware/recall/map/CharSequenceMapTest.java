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

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class CharSequenceMapTest
{
    private static final String SEARCH_TERM = "searchTerm";
    private static final long ID = 17L;
    private static final int INITIAL_SIZE = 16;
    private static final long MISSING_VALUE = Long.MIN_VALUE;
    private static final int MAX_KEY_LENGTH = 64;

    private final CharSequenceMap map = new CharSequenceMap(
        MAX_KEY_LENGTH, INITIAL_SIZE, MISSING_VALUE);
    private final List<Long> receivedList = new ArrayList<>();

    @Test
    void shouldRejectKeyThatIsTooLong()
    {
        final StringBuilder key = new StringBuilder();
        for (int i = 0; i <= MAX_KEY_LENGTH; i++)
        {
            key.append('x');
        }
        Assertions.assertThrows(IllegalArgumentException.class, () -> map.put(key, 7L));
    }

    @Test
    void shouldStoreSingleValue()
    {
        map.put(SEARCH_TERM, ID);

        assertSearchResult(map, SEARCH_TERM, ID);
    }

    @Test
    void shouldNotRetrieveUnknownValue()
    {
        map.get(SEARCH_TERM);

        assertThat(receivedList).isEmpty();
    }

    @Test
    void shouldRetrieveMultipleValuesWhenHashesCollide()
    {
        final CharSequenceMap poorIndex = new CharSequenceMap(16, 10, cs -> 7, Long.MIN_VALUE);

        final String otherTerm = "otherTerm";
        final long otherId = 99L;

        poorIndex.put(SEARCH_TERM, ID);
        poorIndex.put(otherTerm, otherId);

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
            map.put("searchTerm_" + i, i);
            assertThat(map.size()).isEqualTo(i + 1);
        }

        for (int i = 0; i < doubleInitialSize; i++)
        {
            receivedList.clear();
            assertSearchResult(map, "searchTerm_" + i, i);
        }
    }

    @Test
    void shouldReplaceExistingValue()
    {
        final long otherId = 42L;
        map.put(SEARCH_TERM, ID);
        map.put(SEARCH_TERM, otherId);

        assertSearchResult(map, SEARCH_TERM, otherId);
    }

    @Test
    void shouldWrapBuffer()
    {
        final int initialSize = 20;
        final CharSequenceMap map = new CharSequenceMap(64, initialSize, Long.MIN_VALUE);
        final String prefix = "SYM_";
        for (int i = 0; i < initialSize; i++)
        {
            map.put(prefix + i, i);
        }

        for (int i = 0; i < initialSize; i++)
        {
            assertThat(map.get(prefix + i)).isEqualTo(i);
        }
    }

    @Test
    void shouldRemoveValue()
    {
        map.put(SEARCH_TERM, ID);

        assertThat(map.remove(SEARCH_TERM)).isEqualTo(ID);
        assertThat(map.remove(SEARCH_TERM)).isEqualTo(MISSING_VALUE);
        assertThat(map.get(SEARCH_TERM)).isEqualTo(MISSING_VALUE);
        assertThat(map.size()).isEqualTo(0);
    }

    @Disabled
    @Test
    void comparisonTest()
    {
        final Map<String, Long> control = new HashMap<>();
        final Random random = ThreadLocalRandom.current();

        for (int i = 0; i < 10_000; i++)
        {
            final String key = UUID.randomUUID().toString();
            final long value = random.nextLong();
            control.put(key, value);
            map.put(key, value);
        }

        assertThat(map.size()).isEqualTo(10_000);

        Set<String> controlKeys = new HashSet<>(control.keySet());
        int counter = 0;
        for (final String controlKey : controlKeys)
        {
            if ((counter++ & 7) == 0)
            {
                assertThat(map.remove(controlKey)).isEqualTo(control.remove(controlKey));
            }
        }

        assertThat(map.size()).isEqualTo(control.size());

        controlKeys = new HashSet<>(control.keySet());
        for (final String controlKey : controlKeys)
        {
            assertThat(map.get(controlKey)).isEqualTo(control.get(controlKey));
        }

        for (int i = 0; i < 10_000; i++)
        {
            final String key = UUID.randomUUID().toString();
            final long value = random.nextLong();
            control.put(key, value);
            map.put(key, value);
        }

        controlKeys = new HashSet<>(control.keySet());
        for (final String controlKey : controlKeys)
        {
            assertThat(map.get(controlKey)).isEqualTo(control.get(controlKey));
        }
    }

    private void assertSearchResult(final CharSequenceMap index, final String searchTerm, final long retrievedId)
    {
        assertThat(index.get(searchTerm)).isEqualTo(retrievedId);
    }
}