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

package org.radarbase.passive.google.sleep

import android.annotation.SuppressLint
import android.app.PendingIntent
import android.content.BroadcastReceiver
import android.content.Intent
import android.content.IntentFilter
import android.content.pm.PackageManager
import androidx.core.content.ContextCompat
import com.google.android.gms.location.ActivityRecognition
import com.google.android.gms.location.SleepClassifyEvent
import com.google.android.gms.location.SleepSegmentEvent
import com.google.android.gms.location.SleepSegmentRequest
import org.radarbase.android.data.DataCache
import org.radarbase.android.source.AbstractSourceManager
import org.radarbase.android.source.BaseSourceState
import org.radarbase.android.source.SourceStatusListener
import org.radarbase.android.util.CoroutineTaskExecutor
import org.radarbase.android.util.toPendingIntentFlag
import org.radarbase.passive.google.sleep.GoogleSleepProvider.Companion.ACTIVITY_RECOGNITION_COMPAT
import org.radarcns.kafka.ObservationKey
import org.radarcns.passive.google.GoogleSleepClassifyEvent
import org.radarcns.passive.google.GoogleSleepSegmentEvent
import org.radarcns.passive.google.SleepClassificationStatus
import org.slf4j.LoggerFactory

class GoogleSleepManager(context: GoogleSleepService) : AbstractSourceManager<GoogleSleepService, BaseSourceState>(context) {
    private val segmentEventTopic: DataCache<ObservationKey, GoogleSleepSegmentEvent> = createCache(
        "android_google_sleep_segment_event",
        GoogleSleepSegmentEvent()
    )
    private val classifyEventTopic: DataCache<ObservationKey, GoogleSleepClassifyEvent> = createCache(
        "android_google_sleep_classify_event",
        GoogleSleepClassifyEvent()
    )

    private val sleepBroadcastReceiver: BroadcastReceiver
    private val sleepTaskExecutor = CoroutineTaskExecutor(this::class.simpleName!!)
    private val sleepPendingIntent: PendingIntent
    private val isPermissionGranted
    get() = ContextCompat.checkSelfPermission(service,ACTIVITY_RECOGNITION_COMPAT) == PackageManager.PERMISSION_GRANTED

    init {
        name = context.getString(R.string.googleSleepDisplayName)
        sleepBroadcastReceiver = SleepReceiver(this, sleepTaskExecutor)
        sleepPendingIntent = createSleepPendingIntent()
    }

    override fun start(acceptableIds: Set<String>) {
        register()
        sleepTaskExecutor.start()
        status = SourceStatusListener.Status.READY
        sleepTaskExecutor.execute {
            registerSleepReceiver()
            registerForSleepData()
        }
    }

    private fun registerSleepReceiver() {
        val filter = IntentFilter(ACTION_SLEEP_DATA)
        service.registerReceiver(sleepBroadcastReceiver, filter)
        logger.info("registering for the sleep receiver.")
    }

     suspend fun sendSleepSegmentData(sleepIntent: Intent) {
        logger.info("Sleep segment event data received")
        val sleepSegmentEvents: List<SleepSegmentEvent> = SleepSegmentEvent.extractEvents(sleepIntent)
        sleepSegmentEvents.forEach { sleepSegmentEvent ->
            val time = currentTime
            val startTime: Double = sleepSegmentEvent.startTimeMillis / 1000.0
            val endTime: Double = sleepSegmentEvent.endTimeMillis / 1000.0
            val status: SleepClassificationStatus = sleepSegmentEvent.status.toSleepClassificationStatus()

            send(segmentEventTopic, GoogleSleepSegmentEvent(startTime, time, endTime, status))
        }
    }

     suspend fun sendSleepClassifyData(sleepIntent: Intent) {
        logger.info("Sleep classify event data received")
        val sleepClassifyEvents: List<SleepClassifyEvent> = SleepClassifyEvent.extractEvents(sleepIntent)
        sleepClassifyEvents.forEach {  sleepClassifyEvent ->
            val time = sleepClassifyEvent.timestampMillis / 1000.0
            val sleepConfidence: Float = sleepClassifyEvent.confidence.toFloat() / 100
            val light = sleepClassifyEvent.light
            val motion = sleepClassifyEvent.motion

            send(classifyEventTopic, GoogleSleepClassifyEvent(time, currentTime, sleepConfidence, light, motion))
        }
    }

    @SuppressLint("MissingPermission")
    private fun registerForSleepData() {
        if (isPermissionGranted) {
            status = SourceStatusListener.Status.CONNECTING

            ActivityRecognition.getClient(service)
                .requestSleepSegmentUpdates(
                    sleepPendingIntent,
                    SleepSegmentRequest.getDefaultSleepSegmentRequest() )
                .addOnSuccessListener {
                    status = SourceStatusListener.Status.CONNECTED
                    logger.info("Successfully subscribed to sleep data")
                }
                .addOnFailureListener {exception ->
                    disconnect()
                    logger.error("Exception while subscribing to sleep data: $exception")
                }
        }
        else {
            logger.warn("Permission not granted for ACTIVITY_RECOGNITION")
            disconnect()
        }
    }

    private fun createSleepPendingIntent(): PendingIntent {
        val intent = Intent(ACTION_SLEEP_DATA)
        logger.info("Sleep pending intent created")
        return PendingIntent.getBroadcast(service, SLEEP_DATA_REQUEST_CODE, intent,
            PendingIntent.FLAG_CANCEL_CURRENT.toPendingIntentFlag(true))
    }

    private fun unRegisterSleepReceiver() {
        try {
            service.unregisterReceiver(sleepBroadcastReceiver)
            logger.info("Unregistered from sleep receiver ")
        } catch (ex: IllegalArgumentException) {
            logger.error("Exception when unregistering from sleep receiver", ex)
        }
    }

    private fun unRegisterFromSleepData() {
        ActivityRecognition.getClient(service)
            .removeSleepSegmentUpdates(sleepPendingIntent)
            .addOnSuccessListener {
                logger.info("Successfully unsubscribed to sleep data")
            }
            .addOnFailureListener {exception ->
                logger.error("Exception while unsubscribing to sleep data: $exception")
            }
    }

    override fun onClose() {
        sleepTaskExecutor.stop {
            unRegisterFromSleepData()
            unRegisterSleepReceiver()
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(GoogleSleepManager::class.java)

        const val ACTION_SLEEP_DATA = "org.radarbase.passive.google.sleep.ACTION_SLEEP_DATA"
        private const val SLEEP_DATA_REQUEST_CODE = 1197424

        private fun Int.toSleepClassificationStatus(): SleepClassificationStatus = when (this) {
            0 -> SleepClassificationStatus.SUCCESSFUL
            1 -> SleepClassificationStatus.MISSING_DATA
            2 -> SleepClassificationStatus.NOT_DETECTED
            else -> SleepClassificationStatus.UNKNOWN
        }
    }
}
