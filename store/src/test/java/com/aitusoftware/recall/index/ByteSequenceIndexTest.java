package com.aitusoftware.recall.index;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;

class ByteSequenceIndexTest
{
    private static final ByteBuffer SEARCH_TERM = toBuffer("searchTerm");
    private static final long ID = 17L;
    private static final int INITIAL_SIZE = 16;
    private final ByteSequenceIndex index = new ByteSequenceIndex(16, INITIAL_SIZE);
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
        final ByteSequenceIndex poorIndex = new ByteSequenceIndex(16, 10, cs -> 7);

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

    private void assertSearchResult(final ByteSequenceIndex index, final ByteBuffer searchTerm, final long retrievedId)
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