package com.aitusoftware.recall.index;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;

class CharSequenceIndexTest
{
    private static final String SEARCH_TERM = "searchTerm";
    private static final long ID = 17L;
    private static final int INITIAL_SIZE = 16;
    private final CharSequenceIndex index = new CharSequenceIndex(16, INITIAL_SIZE);
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
        final CharSequenceIndex poorIndex = new CharSequenceIndex(16, 10, cs -> 7);

        final String otherTerm = "otherTerm";
        final long otherId = 99L;

        poorIndex.insert(SEARCH_TERM, ID);
        poorIndex.insert(otherTerm, otherId);

        assertSearchResult(poorIndex, SEARCH_TERM, ID);

        receivedList.clear();

        assertSearchResult(poorIndex, otherTerm, otherId);
    }

    @Disabled
    @Test
    void shouldGrowAsRequired()
    {
        for (int i = 0; i < INITIAL_SIZE; i++)
        {
            index.insert("searchTerm_" + i, i);
        }

        final String termToCauseGrowth = "termToCauseGrowth";
        final long otherId = 37L;
        index.insert(termToCauseGrowth, otherId);

        assertSearchResult(index, termToCauseGrowth, otherId);

        for (int i = 0; i < INITIAL_SIZE; i++)
        {
            receivedList.clear();
            assertSearchResult(index, "searchTerm_" + i, i);
        }
    }

    @Disabled
    @Test
    void shouldReplaceExistingValue()
    {
        final long otherId = 42L;
        index.insert(SEARCH_TERM, ID);
        index.insert(SEARCH_TERM, otherId);

        assertSearchResult(index, SEARCH_TERM, otherId);
    }

    private void assertSearchResult(final CharSequenceIndex poorIndex, final String searchTerm, final long retrievedId)
    {
        poorIndex.search(searchTerm, this::onReceivedId);
        assertThat(receivedList).containsExactly(retrievedId);
    }

    private void onReceivedId(final long id)
    {
        receivedList.add(id);
    }
}