package org.radarbase.android.data

import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import org.radarbase.kotlin.coroutines.launchJoin
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.File
import java.io.IOException

class DataCacheGroup<K: Any, V: Any>(
    val activeDataCache: DataCache<K, V>,
    val deprecatedCaches: MutableList<ReadableDataCache>
) {
    val topicName: String = activeDataCache.topic.name

    @Throws(IOException::class)
    suspend fun deleteEmptyCaches() {
        val cacheIterator = deprecatedCaches.iterator()
        while (cacheIterator.hasNext()) {
            val storedCache = cacheIterator.next()
            if (storedCache.numberOfRecords.value > 0) {
                continue
            }
            cacheIterator.remove()
            storedCache.stop()
            val tapeFile = storedCache.file
            if (!tapeFile.delete()) {
                logger.warn("Cannot remove old DataCache file " + tapeFile + " for topic " + storedCache.readTopic.name)
            }
            val name = tapeFile.absolutePath
            val base =
                name.substring(0, name.length - storedCache.serialization.fileExtension.length)
            val keySchemaFile = File(base + CacheStore.KEY_SCHEMA_EXTENSION)
            if (!keySchemaFile.delete()) {
                logger.warn("Cannot remove old key schema file " + keySchemaFile + " for topic " + storedCache.readTopic.name)
            }
            val valueSchemaFile = File(base + CacheStore.VALUE_SCHEMA_EXTENSION)
            if (!valueSchemaFile.delete()) {
                logger.warn("Cannot remove old value schema file " + valueSchemaFile + " for topic " + storedCache.readTopic.name)
            }
        }
    }

    suspend fun stop() {
        coroutineScope {
            deprecatedCaches.forEach {
                launch {
                    it.stop()
                }
            }
            activeDataCache.stop()
        }
    }

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(DataCacheGroup::class.java)
    }
}
