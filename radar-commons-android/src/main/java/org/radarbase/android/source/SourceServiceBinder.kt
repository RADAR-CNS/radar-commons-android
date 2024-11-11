package org.radarbase.android.source

import android.os.Binder
import android.os.Bundle
import android.os.Parcel
import androidx.lifecycle.LiveData
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.zip
import org.radarbase.android.auth.SourceMetadata
import org.radarbase.android.kafka.ServerStatus
import org.radarbase.android.kafka.TopicSendResult
import org.radarbase.data.RecordData
import java.io.IOException
import java.util.*

class SourceServiceBinder<T : BaseSourceState>(private val sourceService: SourceService<T>) : Binder(), SourceBinder<T> {
    override val sourceStatus: StateFlow<SourceStatusListener.Status>
        get() = sourceService.status

    override val sourceConnectFailed: SharedFlow<SourceService.SourceConnectFailed>
        get() = sourceService.sourceConnectFailed

    @Throws(IOException::class)
    override suspend fun getRecords(topic: String, limit: Int): RecordData<Any, Any>? {
        val localDataHandler = sourceService.dataHandler ?: return null
        return localDataHandler.getCache(topic).getRecords(limit)
    }

    override var manualAttributes: Map<String, String>
        get() = sourceService.manualAttributes
        set(value) {
            sourceService.manualAttributes = value
        }

    override val registeredSource: SourceMetadata?
        get() = sourceService.registeredSource

    override val sourceState: T
        get() = sourceService.state

    override val sourceName: String?
        get() = sourceService.sourceManager?.name

    override fun startRecording(acceptableIds: Set<String>) {
        sourceService.startRecording(acceptableIds)
    }

    override fun restartRecording(acceptableIds: Set<String>) {
        sourceService.restartRecording(acceptableIds)
    }

    override fun stopRecording() {
        sourceService.stopRecording()
    }

    override val serverStatus: Flow<ServerStatus>?
        get() = sourceService.dataHandler?.serverStatus

    override val serverRecordsSent: Flow<TopicSendResult>?
        get() = sourceService.dataHandler?.recordsSent

    override fun updateConfiguration(bundle: Bundle) {
        sourceService.onInvocation(bundle)
    }

    override val numberOfRecords: Flow<Long>?
        get() = sourceService.dataHandler?.let { data ->
            val numbers: List<StateFlow<Long>> = data.caches.map { it.numberOfRecords }

                combine(numbers) { records ->
                    records.sum()
                }
        }

    override fun needsBluetooth(): Boolean = sourceService.isBluetoothConnectionRequired

    override fun shouldRemainInBackground(): Boolean = sourceService.state.status !in nonFunctioningStates

    public override fun onTransact(code: Int, data: Parcel, reply: Parcel?, flags: Int): Boolean {
        throw UnsupportedOperationException()
    }

    companion object {
        private val nonFunctioningStates: Set<SourceStatusListener.Status> = EnumSet.of(
            SourceStatusListener.Status.DISCONNECTED,
            SourceStatusListener.Status.UNAVAILABLE,
        )
    }
}
