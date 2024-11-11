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

package org.radarbase.passive.google.activity

import android.annotation.SuppressLint
import android.app.PendingIntent
import android.content.Intent
import android.content.IntentFilter
import android.content.pm.PackageManager
import android.os.SystemClock
import androidx.core.content.ContextCompat
import com.google.android.gms.location.ActivityRecognition
import com.google.android.gms.location.ActivityTransition
import com.google.android.gms.location.ActivityTransitionResult
import com.google.android.gms.location.DetectedActivity
import org.radarbase.android.data.DataCache
import org.radarbase.android.source.AbstractSourceManager
import org.radarbase.android.source.BaseSourceState
import org.radarbase.android.source.SourceStatusListener
import org.radarbase.android.util.CoroutineTaskExecutor
import org.radarbase.android.util.toPendingIntentFlag
import org.radarbase.passive.google.activity.GoogleActivityProvider.Companion.ACTIVITY_RECOGNITION_COMPAT
import org.radarcns.kafka.ObservationKey
import org.radarcns.passive.google.ActivityType
import org.radarcns.passive.google.GoogleActivityTransitionEvent
import org.radarcns.passive.google.TransitionType
import org.slf4j.LoggerFactory

class GoogleActivityManager(context: GoogleActivityService) : AbstractSourceManager<GoogleActivityService, BaseSourceState>(context) {
    private val activityTransitionEventTopic: DataCache<ObservationKey, GoogleActivityTransitionEvent> = createCache(
        "android_google_activity_transition_event",
        GoogleActivityTransitionEvent()
    )

    private val activityTaskExecutor = CoroutineTaskExecutor(this::class.simpleName!!)
    private val activityPendingIntent: PendingIntent
    private val activityTransitionReceiver: ActivityTransitionReceiver

    private val isPermissionGranted: Boolean
        get() = ContextCompat.checkSelfPermission(service, ACTIVITY_RECOGNITION_COMPAT) == PackageManager.PERMISSION_GRANTED

    init {
        name = service.getString(R.string.google_activity_display_name)
        activityTransitionReceiver = ActivityTransitionReceiver(this, activityTaskExecutor)
        activityPendingIntent = createActivityPendingIntent()
    }

    override fun start(acceptableIds: Set<String>) {
        register()
        activityTaskExecutor.start()
        status = SourceStatusListener.Status.READY
        activityTaskExecutor.execute {
            registerActivityTransitionReceiver()
            registerForActivityUpdates()
        }
    }

    private fun registerActivityTransitionReceiver() {
        val filter = IntentFilter(ACTION_ACTIVITY_UPDATE)
        service.registerReceiver(activityTransitionReceiver, filter)
        logger.info("Registered activity transition receiver.")
    }

    @SuppressLint("MissingPermission")
    private fun registerForActivityUpdates() {
        if (isPermissionGranted) {
            status = SourceStatusListener.Status.CONNECTING

            ActivityRecognition.getClient(service)
                    .requestActivityTransitionUpdates(
                        ActivityTransitionUtil.getActivityTransitionRequest(),
                        activityPendingIntent
                    )
                    .addOnSuccessListener {
                        status = SourceStatusListener.Status.CONNECTED
                        logger.info("Successfully subscribed to activity transition updates")
                    }
                    .addOnFailureListener { exception ->
                        disconnect()
                        logger.error("Exception while subscribing for activity transition updates: $exception")                    }
        }
        else {
            logger.warn("Permission not granted for ACTIVITY_RECOGNITION")
            disconnect()
        }
    }

    suspend fun sendActivityTransitionUpdates(activityIntent: Intent) {
        logger.info("Activity transition event data received")
        val activityTransitionResult: ActivityTransitionResult = ActivityTransitionResult.extractResult(activityIntent) ?: return

        // Accepting the events only if the activity happened in last 30 seconds
        val now = SystemClock.elapsedRealtimeNanos()
        activityTransitionResult.transitionEvents.asSequence()
            .filter { now - it.elapsedRealTimeNanos < 30_000_000_000 }
            .forEach { event ->
                val time = event.elapsedRealTimeNanos.toActivityTime() / 1000.0
                val activity = event.activityType.toActivityType()
                val transition = event.transitionType.toTransitionType()
                send(
                    activityTransitionEventTopic,
                    GoogleActivityTransitionEvent(time, currentTime, activity, transition)
                )
            }
    }

    private fun createActivityPendingIntent(): PendingIntent {
        val intent = Intent(ACTION_ACTIVITY_UPDATE)
        logger.info("Activity pending intent created")
        return PendingIntent.getBroadcast(service, ACTIVITY_UPDATE_REQUEST_CODE, intent,
            PendingIntent.FLAG_CANCEL_CURRENT.toPendingIntentFlag(true)
        )
    }

    private fun unRegisterFromActivityReceiver() {
        try {
            service.unregisterReceiver(activityTransitionReceiver)
            logger.info("Unregistered from activity transition receiver ")
        } catch (ex: IllegalArgumentException) {
            logger.error("Exception when unregistering from activity transition receiver", ex)
        }
    }

    @SuppressLint("MissingPermission")
    private fun unRegisterFromActivityUpdates() {
        if (isPermissionGranted) {
            ActivityRecognition.getClient(service)
                .removeActivityTransitionUpdates(activityPendingIntent)
                .addOnSuccessListener {
                    logger.info("Successfully unsubscribed from activity transition updates")
                }
                .addOnFailureListener { exception ->
                    logger.error("Exception while unsubscribing from activity transition updates: $exception")
                }
        }
    }

    override fun onClose() {
        activityTaskExecutor.stop {
            unRegisterFromActivityUpdates()
            unRegisterFromActivityReceiver()
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(GoogleActivityManager::class.java)

        const val ACTION_ACTIVITY_UPDATE = "org.radarbase.passive.google.activity.ACTION_ACTIVITY_UPDATE"
        private const val ACTIVITY_UPDATE_REQUEST_CODE = 534351
    }

    private fun Int.toTransitionType(): TransitionType = when (this) {
        ActivityTransition.ACTIVITY_TRANSITION_ENTER -> TransitionType.ENTER
        ActivityTransition.ACTIVITY_TRANSITION_EXIT -> TransitionType.EXIT
        else -> TransitionType.UNKNOWN
    }

    private fun Int.toActivityType(): ActivityType = when (this) {
        DetectedActivity.IN_VEHICLE -> ActivityType.IN_VEHICLE
        DetectedActivity.ON_BICYCLE -> ActivityType.ON_BICYCLE
        DetectedActivity.ON_FOOT -> ActivityType.ON_FOOT
        DetectedActivity.STILL -> ActivityType.STILL
        DetectedActivity.WALKING -> ActivityType.WALKING
        DetectedActivity.RUNNING -> ActivityType.RUNNING
        else -> ActivityType.UNKNOWN
    }

    /** Returns epoch time (ms) for the transition events from the time after last device boot when this transition happened. */
    private fun Long.toActivityTime(): Long = getLastBootTime() + this / 1000000

    private fun getLastBootTime(): Long = System.currentTimeMillis() - SystemClock.elapsedRealtime()
}




