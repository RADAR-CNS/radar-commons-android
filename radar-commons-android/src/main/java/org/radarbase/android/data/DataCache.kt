/*
 * Copyright 2017 The Hyve
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

package org.radarbase.android.data

import kotlinx.coroutines.flow.MutableStateFlow
import org.radarbase.topic.AvroTopic

interface DataCache<K: Any, V: Any> : ReadableDataCache {
    /** Get the topic the cache stores.  */
    val topic: AvroTopic<K, V>

    /** Add a new measurement to the cache.  */
    suspend fun addMeasurement(key: K, value: V)

    /** Configuration. */
    val config: MutableStateFlow<CacheConfiguration>

    /** Trigger a flush to happen as soon as possible. */
    fun triggerFlush()

    suspend fun flush()
}
