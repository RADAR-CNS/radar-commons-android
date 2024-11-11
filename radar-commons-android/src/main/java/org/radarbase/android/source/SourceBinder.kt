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

import android.os.Bundle
import androidx.lifecycle.LiveData
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import org.radarbase.android.auth.SourceMetadata
import org.radarbase.android.kafka.ServerStatus
import org.radarbase.android.kafka.TopicSendResult
import org.radarbase.android.source.SourceService.SourceConnectFailed
import org.radarbase.data.RecordData
import java.io.IOException

interface SourceBinder<T : BaseSourceState> {
    /** Get the current source status  */
    val sourceState: T?
    /** Get the current source name, or null if unknown.  */
    val sourceName: String?
    /** Get the current server status  */
    val serverStatus: Flow<ServerStatus>?
    /** Get the last number of records sent  */
    val serverRecordsSent: Flow<TopicSendResult>?

    /** Manual attributes set by the user that will be added to a registered source. */
    var manualAttributes: Map<String, String>
    /** Currently connected and registered source. */
    val registeredSource: SourceMetadata?

    /** Start scanning and recording from a compatible source.
     * @param acceptableIds a set of source IDs that may be connected to.
     * If empty, no selection is made.
     */
    fun startRecording(acceptableIds: Set<String>)

    /** Stop recording and immediately start recording after that */
    fun restartRecording(acceptableIds: Set<String>)

    /** Stop scanning and recording  */
    fun stopRecording()

    @Throws(IOException::class)
    suspend fun getRecords(topic: String, limit: Int): RecordData<Any, Any>?

    /** Update the configuration of the service  */
    fun updateConfiguration(bundle: Bundle)

    /** Number of records in cache unsent  */
    val numberOfRecords: Flow<Long>?

    fun needsBluetooth(): Boolean

    fun shouldRemainInBackground(): Boolean
    val sourceStatus: StateFlow<SourceStatusListener.Status>
    val sourceConnectFailed: SharedFlow<SourceConnectFailed>
}
