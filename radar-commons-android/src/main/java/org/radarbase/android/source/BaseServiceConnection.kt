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

package org.radarbase.android.source

import android.content.ComponentName
import android.content.ServiceConnection
import android.os.Bundle
import android.os.IBinder
import androidx.lifecycle.LiveData
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import org.radarbase.android.auth.SourceMetadata
import org.radarbase.android.kafka.ServerStatus
import org.radarbase.android.kafka.TopicSendResult
import org.radarbase.android.util.equalTo
import org.radarbase.data.RecordData
import org.radarbase.util.Strings
import org.slf4j.LoggerFactory
import java.io.IOException

open class BaseServiceConnection<S : BaseSourceState>(protected val serviceClassName: String) : ServiceConnection {
    @get:Synchronized
    protected var serviceBinder: SourceBinder<S>? = null
        private set

    val sourceName: String?
        get() = serviceBinder?.sourceName

    val isRecording: Boolean
        get() = sourceStatus?.value !in arrayOf(
            SourceStatusListener.Status.DISCONNECTED,
            SourceStatusListener.Status.UNAVAILABLE,
        )

    val serverStatus: Flow<ServerStatus>?
        get() = serviceBinder?.serverStatus

    val serverSent: Flow<TopicSendResult>?
        get() = serviceBinder?.serverRecordsSent

    val sourceState: S?
        get() = serviceBinder?.sourceState

    var manualAttributes: Map<String, String>?
        get() = serviceBinder?.manualAttributes
        set(value) {
            value ?: return
            serviceBinder?.manualAttributes = value
        }

    val sourceStatus: StateFlow<SourceStatusListener.Status>?
        get() = serviceBinder?.sourceStatus

    val sourceConnectFailed: SharedFlow<SourceService.SourceConnectFailed>?
        get() = serviceBinder?.sourceConnectFailed

    val registeredSource: SourceMetadata?
        get() = serviceBinder?.registeredSource

    init {
        this.serviceBinder = null
    }

    override fun onServiceConnected(className: ComponentName?, service: IBinder?) {
        if (serviceBinder == null && service != null) {
            logger.debug("Bound to service {}", className)
            try {
                @Suppress("UNCHECKED_CAST")
                serviceBinder = service as SourceBinder<S>
            } catch (ex: ClassCastException) {
                logger.error("Cannot process remote source services.", ex)
            }
        } else {
            logger.info("Trying to re-bind service, from {} to {}", serviceBinder, service)
        }
    }

    @Throws(IOException::class)
    suspend fun getRecords(topic: String, limit: Int): RecordData<Any, Any>? =
        serviceBinder?.getRecords(topic, limit)

    /**
     * Start looking for sources to record.
     * @param acceptableIds case insensitive parts of source ID's that are allowed to connect.
     */
    fun startRecording(acceptableIds: Set<String>) {
        try {
            serviceBinder?.startRecording(acceptableIds)
        } catch (ex: IllegalStateException) {
            logger.error("Cannot start service {}: {}", this, ex.message)
        }
    }

    fun restartRecording(acceptableIds: Set<String>) = try {
        serviceBinder?.restartRecording(acceptableIds)
    } catch (ex: IllegalStateException) {
        logger.error("Cannot restart service {}: {}", this, ex.message)
    }

    fun stopRecording() {
        serviceBinder?.stopRecording()
    }

    fun hasService(): Boolean = serviceBinder != null

    override fun onServiceDisconnected(className: ComponentName?) {
        // only do these steps once
        if (hasService()) {
            synchronized(this) {
                serviceBinder = null
            }
        }
    }

    fun updateConfiguration(bundle: Bundle) {
        serviceBinder?.updateConfiguration(bundle)
    }

    fun numberOfRecords(): Flow<Long>? = serviceBinder?.numberOfRecords

    /**
     * True if given string is a substring of the source name.
     */
    fun isAllowedSource(values: Collection<String>): Boolean {
        if (values.isEmpty()) {
            return true
        }
        val idOptions = listOfNotNull(
            serviceBinder?.sourceName,
            serviceBinder?.sourceState?.id?.sourceId,
        )

        return idOptions.isNotEmpty() && values
            .map(Strings::containsIgnoreCasePattern)
            .any { pattern -> idOptions.any { pattern.matcher(it).find() } }
    }

    fun needsBluetooth(): Boolean {
        return serviceBinder?.needsBluetooth() == true
    }

    fun mayBeDisabledInBackground(): Boolean {
        return serviceBinder?.shouldRemainInBackground() == false
    }

    override fun equals(other: Any?) = equalTo(other, BaseServiceConnection<*>::serviceClassName)

    override fun hashCode(): Int = serviceClassName.hashCode()

    override fun toString(): String = "ServiceConnection<$serviceClassName>"

    companion object {
        private val logger = LoggerFactory.getLogger(BaseServiceConnection::class.java)
    }
}
