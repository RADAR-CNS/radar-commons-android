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

import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import org.radarbase.android.kafka.ServerStatus
import org.radarbase.android.kafka.ServerStatusListener
import org.radarbase.android.kafka.TopicSendReceipt
import org.radarbase.topic.AvroTopic

interface DataHandler<K: Any, V: Any>: ServerStatusListener {
    /** Get all caches.  */
    val caches: List<ReadableDataCache>

    /** Get caches currently active for sending.  */
    val activeCaches: List<DataCacheGroup<*, *>>

    override val serverStatus: MutableStateFlow<ServerStatus>

    override val recordsSent: MutableSharedFlow<TopicSendReceipt>

    suspend fun <W: V> registerCache(topic: AvroTopic<K, W>): DataCache<K, W>

    fun handler(build: DataHandlerConfiguration.() -> Unit)
    fun getCache(topic: String): DataCache<*, *>
    suspend fun flushCaches(successCallback: () -> Unit, errorCallback: () -> Unit)
    val numberOfRecords: SharedFlow<TableDataHandler.CacheSize>
}
