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

public interface SequenceMap<T>
{
    /**
     * Insert a value into the map.
     *
     * @param value value to use as a key
     * @param id    id to store
     */
    void put(T value, long id);

    /**
     * Searches the map for a given key.
     *
     * @param value the key to search for
     * @return the retrieved value, or {@code missingValue} if it was not present
     */
    long get(T value);

    /**
     * Removes an entry for a given key.
     *
     * @param value the key to search for
     * @return the stored value, or {@code missingValue} if the key was not present
     */
    long remove(T value);

    /**
     * Returns the number of entries in the map.
     *
     * @return the number of entries
     */
    int size();

    /**
     * Allocates a new buffer and copies existing entries to it.
     */
    void rehash();
}
