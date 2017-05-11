/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionKeySerde;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;

/**
 * Merges two iterators. Assumes each of them is sorted by key
 *
 * @param <K>
 * @param <AGG>
 */
class MergedSortedCacheSessionStoreIterator<K, AGG> extends AbstractMergedSortedCacheStoreIterator<Windowed<K>, Windowed<Bytes>, AGG, byte[]> {

    private final StateSerdes<K, AGG> serdes;

    MergedSortedCacheSessionStoreIterator(final PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator,
                                          final KeyValueIterator<Windowed<Bytes>, byte[]> storeIterator,
                                          final StateSerdes<K, AGG> serdes) {
        super(cacheIterator, storeIterator);
        this.serdes = serdes;
    }

    @Override
    public KeyValue<Windowed<K>, AGG> deserializeStorePair(KeyValue<Windowed<Bytes>, byte[]> pair) {
        final K key = serdes.keyFrom(pair.key.key().get());
        return KeyValue.pair(new Windowed<>(key, pair.key.window()), serdes.valueFrom(pair.value));
    }

    @Override
    Windowed<K> deserializeCacheKey(final Bytes cacheKey) {
        return SessionKeySerde.from(cacheKey.get(), serdes.keyDeserializer(), serdes.topic());
    }


    @Override
    AGG deserializeCacheValue(Bytes cacheKey, LRUCacheEntry cacheEntry) {
        return serdes.valueFrom(cacheEntry.value);
    }

    @Override
    public Windowed<K> deserializeStoreKey(Windowed<Bytes> key) {
        final K originalKey = serdes.keyFrom(key.key().get());
        return new Windowed<>(originalKey, key.window());
    }

    @Override
    public int compare(Bytes cacheKey, Windowed<Bytes> storeKey) {
        Bytes storeKeyBytes = SessionKeySerde.bytesToBinary(storeKey);
        return cacheKey.compareTo(storeKeyBytes);
    }
}

