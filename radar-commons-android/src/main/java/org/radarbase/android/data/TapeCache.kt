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

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import org.apache.avro.Schema
import org.radarbase.android.data.serialization.SerializationFactory
import org.radarbase.data.AvroRecordData
import org.radarbase.data.Record
import org.radarbase.data.RecordData
import org.radarbase.topic.AvroTopic
import org.radarbase.util.BackedObjectQueue
import org.radarbase.util.QueueFile
import org.slf4j.LoggerFactory
import java.io.File
import java.io.IOException
import java.util.*
import java.util.concurrent.ExecutionException
import kotlin.coroutines.CoroutineContext

/**
 * Caches measurement on a BackedObjectQueue. Internally, all data is first cached on a local queue,
 * before being written in batches to the BackedObjectQueue, using a single-threaded
 * ExecutorService. Data is retrieved and removed from the queue in a blocking way using that same
 * ExecutorService. Sent messages are not kept, they are immediately removed.
 *
 * @param K measurement key type
 * @param V measurement value type
 * @param topic Kafka Avro topic to write data for.
 * @throws IOException if a BackedObjectQueue cannot be created.
 */
class TapeCache<K: Any, V: Any>(
    override val file: File,
    override val topic: AvroTopic<K, V>,
    override val readTopic: AvroTopic<Any, Any>,
    override val serialization: SerializationFactory,
    config: CacheConfiguration,
    coroutineContext: CoroutineContext = Dispatchers.Default,
) : DataCache<K, V> {

    private val measurementsToAdd = mutableListOf<Record<K, V>>()
    private val serializer = serialization.createSerializer(topic)
    private val deserializer = serialization.createDeserializer(readTopic)

    private lateinit var queueFile: QueueFile
    private lateinit var queue: BackedObjectQueue<Record<K, V>, Record<Any, Any>>
    private val queueFileFactory: CacheConfiguration.QueueFileFactory = config.queueFileType

    private var addMeasurementFuture: Job? = null
    private var configObserverJob: Job? = null

    private val mutex: Mutex = Mutex()
    override val numberOfRecords: MutableStateFlow<Long> = MutableStateFlow(0)
    override val config = MutableStateFlow(config)

    private val job = SupervisorJob()
    private val cacheScope = CoroutineScope(coroutineContext + job + CoroutineName("cache-${topic.name}"))

    private val maximumSize: Long
        get() = config.value.maximumSize.takeIf { it <= Int.MAX_VALUE } ?: Int.MAX_VALUE.toLong()

    override val readUserIdField: Schema.Field?
        get() = topic.keySchema.takeIf { it.type == Schema.Type.RECORD }
            ?.getField("userId")

    init {
        cacheScope.launch(Dispatchers.IO) {
            mutex.withLock {
                queueFile = try {
                    queueFileFactory.generate(Objects.requireNonNull(file), maximumSize)
                } catch (ex: IOException) {
                    logger.error("TapeCache {} was corrupted. Removing old cache.", file, ex)
                    if (file.delete()) {
                        queueFileFactory.generate(file, maximumSize)
                    } else {
                        throw ex
                    }
                }
                queue = BackedObjectQueue(queueFile, serializer, deserializer)
                numberOfRecords.value = queue.size.toLong()
            }
        }

        configObserverJob = cacheScope.launch {
            this@TapeCache.config.collect {
                ensureActive()
                queueFile.maximumFileSize = it.maximumSize
            }
        }
    }

    @Throws(IOException::class)
    override suspend fun getUnsentRecords(limit: Int, sizeLimit: Long): RecordData<Any, Any>? {
        logger.debug("Trying to retrieve records from topic {}", topic.name)
        return withContext(cacheScope.coroutineContext) {
            mutex.withLock {
                try {
                    getValidUnsentRecords(limit, sizeLimit)
                        ?.let { (key, values) ->
                            AvroRecordData(readTopic, key, values)
                        }
                } catch (ex: IOException) {
                    withContext(Dispatchers.IO) {
                        fixCorruptQueue(ex)
                    }
                    null
                } catch (ex: IllegalStateException) {
                    withContext(Dispatchers.IO) {
                        fixCorruptQueue(ex)
                    }
                    null
                }
            }
        }
    }

    private suspend fun getValidUnsentRecords(limit: Int, sizeLimit: Long): Pair<Any, List<Any>>? {
        var currentKey: Any? = null
        lateinit var records: List<Record<Any, Any>?>

        while (currentKey == null) {
            records = withContext(Dispatchers.IO) {
                queue.peek(limit, sizeLimit)
            }

            if (records.isEmpty()) return null

            val nullSize = records.indexOfFirst { it != null }
                    .takeIf { it != -1 }
                    ?: records.size

            if (nullSize > 0) {
                withContext(Dispatchers.IO) {
                    queue -= nullSize
                }
                records = records.subList(nullSize, records.size)
            }
            currentKey = records.firstOrNull()?.key
        }

        val differentKeyIndex = records.indexOfFirst { it?.key != currentKey }
        if (differentKeyIndex > 0) {
            records = records.subList(0, differentKeyIndex)
        }

        return Pair(currentKey, records.mapNotNull { it?.value })
    }

    @Throws(IOException::class)
    override suspend fun getRecords(limit: Int): RecordData<Any, Any>? {
        return getUnsentRecords(limit, maximumSize)?.let { records ->
            AvroRecordData(records.topic, records.key, records.filterNotNull())
        }
    }

    @Throws(IOException::class)
    override suspend fun remove(number: Int) {
        withContext(cacheScope.coroutineContext + Dispatchers.IO) {
            mutex.withLock {
                val actualRemoveSize = number.coerceAtMost(queue.size)
                if (actualRemoveSize > 0) {
                    logger.debug(
                        "Removing {} records from topic {}",
                        actualRemoveSize,
                        topic.name
                    )
                    queue -= actualRemoveSize
                    numberOfRecords.value -= actualRemoveSize
                }
            }
        }
    }

    override suspend fun addMeasurement(key: K, value: V) {
        val record = Record(key, value)

        require(serializer.canSerialize(record)) {
            "Cannot send invalid record to topic $topic with {key: $key, value: $value}"
        }

        mutex.withLock {
            measurementsToAdd += record
            if (addMeasurementFuture == null) {
                addMeasurementFuture = cacheScope.launch {
                    delay(config.value.commitRate)
                    doFlush()
                }
            }
        }
    }

    @Throws(IOException::class)
    override suspend fun stop() {
        flush()
        configObserverJob?.cancelAndJoin()
        cacheScope.cancel()
        queue.close()
    }

    override suspend fun flush() {
        try {
            mutex.withLock {
                val future = addMeasurementFuture
                if (future != null) {
                    future.cancelAndJoin()
                    addMeasurementFuture = null
                    doFlush()
                }
            }
        } catch (e: InterruptedException) {
            logger.warn("Did not wait for adding measurements to complete.")
        } catch (ex: ExecutionException) {
            logger.warn("Failed to execute flush task", ex)
        }
    }

    override fun triggerFlush() {
        cacheScope.launch {
            mutex.withLock {
                val future = addMeasurementFuture
                if (future != null) {
                    future.cancelAndJoin()
                    addMeasurementFuture = null
                    doFlush()
                }
            }
        }
    }

    private suspend fun doFlush() {
        addMeasurementFuture = null

        if (measurementsToAdd.isEmpty()) {
            return
        }
        try {
            logger.info("Writing {} records to file in topic {}.", measurementsToAdd.size, topic.name)
            withContext(Dispatchers.IO) {
                queue += measurementsToAdd
                numberOfRecords.value += measurementsToAdd.size
            }
        } catch (ex: IOException) {
            logger.error("Failed to add records", ex)
            throw RuntimeException(ex)
        } catch (ex: IllegalStateException) {
            logger.error("Queue {} is full, not adding records.", topic.name)
        } catch (ex: IllegalArgumentException) {
            logger.error("Failed to validate all records; adding individual records instead", ex)
            try {
                logger.info("Writing {} records to file in topic {}", measurementsToAdd.size, topic.name)
                for (record in measurementsToAdd) {
                    try {
                        queue += record
                    } catch (ex2: IllegalArgumentException) {
                        logger.error("Failed to write individual record {}", record, ex)
                    }
                }
            } catch (illEx: IllegalStateException) {
                logger.error("Queue {} is full, not adding records", topic.name)
            } catch (ex2: IOException) {
                logger.error("Failed to add record", ex)
                throw RuntimeException(ex)
            }
        } finally {
            measurementsToAdd.clear()
        }
    }

    @Throws(IOException::class)
    private fun fixCorruptQueue(ex: Exception) {
        logger.error("Queue {} was corrupted. Removing cache.", topic.name, ex)
        try {
            queue.close()
        } catch (ioex: IOException) {
            logger.warn("Failed to close corrupt queue", ioex)
        }

        if (file.delete()) {
            queueFile = queueFileFactory.generate(file, maximumSize)
            queue = BackedObjectQueue(queueFile, serializer, deserializer)
            numberOfRecords.value = 0L
        } else {
            throw IOException("Cannot create new cache.")
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(TapeCache::class.java)
    }
}
