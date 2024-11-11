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

package org.radarbase.android.kafka

import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.apache.avro.SchemaValidationException
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.specific.SpecificRecord
import org.radarbase.android.data.DataCacheGroup
import org.radarbase.android.data.DataHandler
import org.radarbase.android.data.ReadableDataCache
import org.radarbase.data.AvroRecordData
import org.radarbase.data.RecordData
import org.radarbase.producer.AuthenticationException
import org.radarbase.producer.KafkaTopicSender
import org.radarbase.producer.rest.ConnectionState
import org.radarbase.producer.rest.RestKafkaSender
import org.radarbase.topic.AvroTopic
import org.radarcns.kafka.ObservationKey
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.IOException
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.CoroutineContext

/**
 * Separate thread to read from the database and send it to the Kafka server. It cleans the
 * database.
 *
 * It uses a set of timers to addMeasurement data and clean the databases.
 */
class KafkaDataSubmitter(
    private val dataHandler: DataHandler<ObservationKey, SpecificRecord>,
    private val sender: RestKafkaSender,
    config: SubmitterConfiguration,
    submitterCoroutineContext: CoroutineContext = Dispatchers.Default
) : Closeable {

    private val topicSenders: MutableMap<String, KafkaTopicSender<Any, Any>> = ConcurrentHashMap()
    private val connectionChecker: KafkaConnectionChecker

    private val submitterExceptionHandler: CoroutineExceptionHandler = CoroutineExceptionHandler{ _, throwable ->
        logger.error("CoroutineExceptionHandler - Error in KafkaDataSubmitter: ", throwable)
    }
    private val submitterJob: Job = SupervisorJob()
    private val submitterScope: CoroutineScope = CoroutineScope(
        submitterCoroutineContext + submitterJob + submitterExceptionHandler + CoroutineName("data-submitter")
    )

    var config: SubmitterConfiguration = config
        set(newValue) {
            submitterScope.launch {
                if (newValue == field) return@launch

                validate(newValue)
                field = newValue.copy()
                schedule()
            }
        }

    private var uploadFuture: Job? = null
    private var uploadIfNeededFuture: Job? = null
    /** Upload rate in milliseconds.  */

    init {
        validate(config)

        logger.debug("Started data submission executor")

        connectionChecker = KafkaConnectionChecker(sender, dataHandler, config.uploadRate * 5)

        submitterScope.launch {
            uploadFuture = null
            uploadIfNeededFuture = null

            try {
                submitterScope.launch {
                    connectionChecker.initialize()
                    if (sender.connectionState.first() == ConnectionState.State.CONNECTED) {
                        dataHandler.serverStatus.value = ServerStatus.CONNECTED
                        connectionChecker.didConnect()
                    } else {
                        dataHandler.serverStatus.value = ServerStatus.DISCONNECTED
                        connectionChecker.didDisconnect(null)
                    }
                }
            } catch (ex: AuthenticationException) {
                connectionChecker.didDisconnect(ex)
            }

            schedule()
        }
    }

    private fun validate(config: SubmitterConfiguration) {
        requireNotNull(config.userId) { "User ID is mandatory to start KafkaDataSubmitter" }
    }

    private suspend fun schedule() {
        val uploadRate = config.uploadRate * config.uploadRateMultiplier * 1000L

        uploadFuture?.also {
            it.cancel()
            uploadFuture = null
        }

        uploadIfNeededFuture?.also {
            it.cancel()
            uploadIfNeededFuture = null
        }

        // Get upload frequency from system property
        uploadFuture = submitterScope.launch {
            while (isActive) {
                delay(uploadRate)
                uploadAllCaches()
            }
        }

        uploadIfNeededFuture = submitterScope.launch {
            while (isActive) {
                delay(uploadRate / 5)
                uploadFullCaches()
            }
        }
    }

    private suspend fun uploadAllCaches() {
        val topicsToSend = dataHandler.activeCaches.mapTo(HashSet()) { it.topicName }
        while (connectionChecker.isConnected.value && topicsToSend.isNotEmpty()) {
            logger.debug("Uploading topics {}", topicsToSend)
            uploadCaches(topicsToSend)
        }
    }

    private suspend fun uploadFullCaches() {
        var sendAgain = true
        while (connectionChecker.isConnected.value && sendAgain) {
            logger.debug("Uploading full topics")
            sendAgain = uploadCachesIfNeeded()
        }
    }

    suspend fun flush(successCallback: () -> Unit, errorCallback: () -> Unit) {
        this.submitterScope.launch {
            uploadAllCaches()
            if (dataHandler.serverStatus.value == ServerStatus.CONNECTED) {
                successCallback()
            } else {
                errorCallback()
            }
        }
    }

    /**
     * Close the submitter eventually. This does not flush any caches.
     */
    override fun close() {
        topicSenders.clear()
        uploadFuture?.also {
            it.cancel()
            uploadFuture = null
        }

        uploadIfNeededFuture?.also {
            it.cancel()
            uploadFuture = null
        }

        connectionChecker.stopHeartbeats()
    }

    /** Get a sender for a topic. Per topic, only ONE thread may use this.  */
    @Throws(IOException::class, SchemaValidationException::class)
    private fun sender(
        topic: AvroTopic<Any, Any>,
    ): KafkaTopicSender<Any, Any> = topicSenders.computeIfAbsentKt(topic.name) { sender.sender(topic) }

    private fun <K: Any, V: Any> MutableMap<K, V>.computeIfAbsentKt(
        key: K,
        mappingFunction: (K) -> V,
    ): V = get(key)
        ?: mappingFunction(key).also { put(key, it) }

    /**
     * Check the connection status eventually.
     */
    suspend fun checkConnection() {
        connectionChecker.check()
    }

    /**
     * Upload the caches if they would cause the buffer to overflow
     */
    private suspend fun uploadCachesIfNeeded(): Boolean {
        var sendAgain = false

        try {
            val uploadingNotified = AtomicBoolean(false)

            for (entry in dataHandler.activeCaches) {
                val unsent = entry.activeDataCache.numberOfRecords
                if (unsent.value > config.amountLimit) {
                    val sent = uploadCache(entry.activeDataCache, uploadingNotified)
                    if (unsent.value - sent > config.amountLimit) {
                        sendAgain = true
                    }
                }
            }
            if (uploadingNotified.get()) {
                dataHandler.serverStatus.value = ServerStatus.CONNECTED
                connectionChecker.didConnect()
            }
        } catch (ex: Exception) {
            connectionChecker.didDisconnect(ex)
            sendAgain = false
        }

        return sendAgain
    }

    /**
     * Upload a limited amount of data stored in the database which is not yet sent.
     */
    private suspend fun uploadCaches(toSend: MutableSet<String>) {
        try {
            val uploadingNotified = AtomicBoolean(false)
            toSend -= dataHandler.activeCaches
                .filter { group ->
                    if (group.topicName in toSend) {
                        val sentActive = uploadCache(group.activeDataCache, uploadingNotified)
                        val sentDeprecated = group.deprecatedCaches.map { uploadCache(it, uploadingNotified) }

                        if (sentDeprecated.any { it == 0 }) {
                            group.deleteEmptyCaches()
                        }
                        sentActive < config.amountLimit
                                && sentDeprecated.all { it < config.amountLimit }
                    } else false
                }
                .mapTo(HashSet(), DataCacheGroup<*,*>::topicName)

            if (uploadingNotified.get()) {
                dataHandler.serverStatus.value = ServerStatus.CONNECTED
                connectionChecker.didConnect()
            }
        } catch (ex: Exception) {
            connectionChecker.didDisconnect(ex)
        }
    }

    /**
     * Upload some data from a single table.
     * @return number of records sent.
     */
    @Throws(IOException::class, SchemaValidationException::class)
    private suspend fun uploadCache(cache: ReadableDataCache, uploadingNotified: AtomicBoolean): Int {
        val data = cache.getUnsentRecords(config.amountLimit, config.sizeLimit)
            ?: return 0

        val size = data.size()
        if (size == 0) {
            return 0
        }

        val recordsNotNull = data.filterNotNull()

        if (recordsNotNull.isNotEmpty()) {
            val topic = cache.readTopic

            val recordUserId = data.userId(cache)

            if (recordUserId == null || recordUserId == config.userId) {
                if (uploadingNotified.compareAndSet(false, true)) {
                    dataHandler.serverStatus.value = ServerStatus.UPLOADING
                }

                try {
                    sender(topic).run {
                        send(AvroRecordData(data.topic, data.key, recordsNotNull))
                    }
                    dataHandler.recordsSent.emit(TopicSendReceipt(topic.name, size.toLong()))
                } catch (ex: AuthenticationException) {
                    dataHandler.recordsSent.emit(TopicSendReceipt(topic.name, -1))
                    throw ex
                } catch (e: Exception) {
                    dataHandler.serverStatus.value = ServerStatus.UPLOADING_FAILED
                    dataHandler.recordsSent.emit(TopicSendReceipt(topic.name, -1))
                    throw e
                }

                logger.debug("uploaded {} {} records", size, topic.name)
            } else {
                logger.warn(
                    "ignoring and removing records for user {} in topic {}",
                    recordUserId,
                    topic.name,
                )
            }
        }

        cache.remove(size)

        return size
    }

    companion object {
        private val logger = LoggerFactory.getLogger(KafkaDataSubmitter::class.java)

        private fun RecordData<*, *>.userId(cache: ReadableDataCache): String? {
            val userIdField = cache.readUserIdField ?: return null
            val recordKey = (key as? IndexedRecord) ?: return null
            return recordKey.get(userIdField.pos()).toString()
        }
    }
}
