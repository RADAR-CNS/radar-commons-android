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

package org.radarbase.passive.phone.usage

import android.app.usage.UsageEvents
import android.app.usage.UsageEvents.Event.*
import android.app.usage.UsageStatsManager
import android.content.*
import android.os.Build
import org.radarbase.android.data.DataCache
import org.radarbase.android.source.AbstractSourceManager
import org.radarbase.android.source.BaseSourceState
import org.radarbase.android.source.SourceStatusListener
import org.radarbase.android.util.OfflineProcessor
import org.radarcns.kafka.ObservationKey
import org.radarcns.passive.phone.PhoneInteractionState
import org.radarcns.passive.phone.PhoneUsageEvent
import org.radarcns.passive.phone.PhoneUserInteraction
import org.radarcns.passive.phone.UsageEventType
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.TimeUnit

class PhoneUsageManager(context: PhoneUsageService) : AbstractSourceManager<PhoneUsageService, BaseSourceState>(context) {

    private val usageEventTopic: DataCache<ObservationKey, PhoneUsageEvent>?
    private val userInteractionTopic: DataCache<ObservationKey, PhoneUserInteraction> = createCache("android_phone_user_interaction", PhoneUserInteraction())

    private val phoneStateReceiver: BroadcastReceiver
    private val usageStatsManager: UsageStatsManager?
    private val preferences: SharedPreferences
    private val phoneUsageProcessor: OfflineProcessor

    private var lastPackageName: String? = null
    private var lastTimestamp: Long = 0
    private var lastEventType: Int = 0
    private var lastEventIsSent: Boolean = false

    init {
        name = service.getString(R.string.phoneUsageServiceDisplayName)
        this.usageStatsManager = context.getSystemService(Context.USAGE_STATS_SERVICE) as? UsageStatsManager

        usageEventTopic = if (usageStatsManager != null) {
            createCache("android_phone_usage_event", PhoneUsageEvent())
        } else {
            logger.warn("Usage statistics are not available.")
            null
        }
        this.preferences = context.getSharedPreferences(PhoneUsageService::class.java.name, Context.MODE_PRIVATE)
        this.loadLastEvent()

        // Listen for screen lock/unlock events
        phoneStateReceiver = object : BroadcastReceiver() {
            override fun onReceive(context: Context, intent: Intent?) {
                // If previous event was a shutdown, then this action indicates that the phone has booted
                if (preferences.getString(LAST_USER_INTERACTION, "") == Intent.ACTION_SHUTDOWN) {
                    sendInteractionState(ACTION_BOOT)
                }
                intent ?: return
                val action = intent.action ?: return

                sendInteractionState(action)
            }
        }

        phoneUsageProcessor = OfflineProcessor(context) {
            process = listOf(this@PhoneUsageManager::processUsageEvents)
            requestCode = USAGE_EVENT_REQUEST_CODE
            requestName = ACTION_UPDATE_EVENTS
            wake = false
        }
    }

    override fun start(acceptableIds: Set<String>) {
        status = SourceStatusListener.Status.READY
        // Start query of usage events
        register()

        // Activity to perform when alarm is triggered
        service.registerReceiver(phoneStateReceiver, IntentFilter().apply {
            addAction(Intent.ACTION_USER_PRESENT) // unlock
            addAction(Intent.ACTION_SCREEN_OFF) // lock
            addAction(Intent.ACTION_SHUTDOWN) // shutdown
        })

        phoneUsageProcessor.start()

        status = SourceStatusListener.Status.CONNECTED
    }

    private fun sendInteractionState(action: String) {
        val state: PhoneInteractionState = when (action) {
            Intent.ACTION_SCREEN_OFF -> PhoneInteractionState.STANDBY
            Intent.ACTION_USER_PRESENT -> PhoneInteractionState.UNLOCKED
            Intent.ACTION_SHUTDOWN -> PhoneInteractionState.SHUTDOWN
            ACTION_BOOT -> PhoneInteractionState.BOOTED
            else -> return
        }

        val time = currentTime
        send(userInteractionTopic, PhoneUserInteraction(time, time, state))

        // Save the last user interaction state. Value shutdown is used to register boot.
        preferences.edit()
                .putString(LAST_USER_INTERACTION, action)
                .apply()
        logger.info("Interaction State: {} {}", time, state)
    }

    /**
     * Set the interval in which to collect logs about app usage.
     * @param interval collection interval in seconds
     */
    fun setUsageEventUpdateRate(interval: Long, unit: TimeUnit) {
        phoneUsageProcessor.interval(interval, unit)
        logger.info("Usage event alarm activated and set to a period of {} seconds", interval)
    }

    private fun processUsageEvents() {
        usageStatsManager ?: return

        // Get events from previous event to now or from a fixed history
        val usageEvents = usageStatsManager.queryEvents(lastTimestamp, System.currentTimeMillis())

        // Loop through all events, send opening and closing of app
        // Assume events are ordered on timestamp in ascending order (old to new)
        val event = UsageEvents.Event()
        while (usageEvents.getNextEvent(event) && !phoneUsageProcessor.isDone) {
            // Ignore config changes, old events, and events from the same activity
            if (event.eventType == CONFIGURATION_CHANGE || event.timeStamp < lastTimestamp) {
                continue
            }

            if (event.packageName == lastPackageName) {
                updateLastEvent(event, false)
            } else {
                // send this closing event
                if (lastPackageName != null && !lastEventIsSent) {
                    sendLastEvent()
                }

                updateLastEvent(event, true)

                // Send the opening of new event
                sendLastEvent()
            }
        }

        // Store the last previous event on internal memory for the next run
        this.storeLastEvent()
    }

    private fun sendLastEvent() {
        usageEventTopic ?: return

        // Event type conversion to Schema defined
        val usageEventType = when (lastEventType) {
            ACTIVITY_RESUMED -> UsageEventType.FOREGROUND
            ACTIVITY_PAUSED -> UsageEventType.BACKGROUND
            CONFIGURATION_CHANGE -> UsageEventType.CONFIG
            SHORTCUT_INVOCATION_COMPAT -> UsageEventType.SHORTCUT
            USER_INTERACTION -> UsageEventType.INTERACTION
            FOREGROUND_SERVICE_START -> UsageEventType.FOREGROUND_SERVICE_START
            FOREGROUND_SERVICE_STOP -> UsageEventType.FOREGROUND_SERVICE_STOP
            STANDBY_BUCKET_CHANGED -> UsageEventType.STANDBY_BUCKET_CHANGED
            ACTIVITY_STOPPED -> UsageEventType.STOPPED
            else -> UsageEventType.OTHER
        }

        val time = lastTimestamp / 1000.0
        val value = PhoneUsageEvent(time, currentTime, lastPackageName, null, null, usageEventType)
        send(usageEventTopic, value)

        if (logger.isDebugEnabled) {
            logger.debug("Event: [{}] {}\n\t{}", lastEventType, lastPackageName, Date(lastTimestamp))
        }
    }

    private fun updateLastEvent(event: UsageEvents.Event, isSent: Boolean) {
        lastPackageName = event.packageName
        lastTimestamp = event.timeStamp
        lastEventType = event.eventType
        lastEventIsSent = isSent
    }

    private fun storeLastEvent() {
        preferences.edit()
            .putString(LAST_PACKAGE_NAME, lastPackageName)
            .putLong(LAST_EVENT_TIMESTAMP, lastTimestamp)
            .putInt(LAST_EVENT_TYPE, lastEventType)
            .putBoolean(LAST_EVENT_IS_SENT, lastEventIsSent)
            .apply()
    }

    private fun loadLastEvent() {
        lastPackageName = preferences.getString(LAST_PACKAGE_NAME, null)
        lastTimestamp = preferences.getLong(LAST_EVENT_TIMESTAMP, System.currentTimeMillis())
        lastEventType = preferences.getInt(LAST_EVENT_TYPE, 0)
        lastEventIsSent = preferences.getBoolean(LAST_EVENT_IS_SENT, true)

        if (lastPackageName == null) {
            logger.info("No previous event details stored")
        }
    }

    override fun onClose() {
        if (phoneUsageProcessor.isStarted) {
            phoneUsageProcessor.close()
            service.unregisterReceiver(phoneStateReceiver)
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(PhoneUsageManager::class.java)

        private val SHORTCUT_INVOCATION_COMPAT = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.N_MR1) SHORTCUT_INVOCATION else 8

        private const val LAST_PACKAGE_NAME = "org.radarcns.phone.packageName"
        private const val LAST_EVENT_TIMESTAMP = "org.radarcns.phone.timestamp"
        private const val LAST_EVENT_TYPE = "org.radarbase.passive.phone.usage.PhoneUsageManager.lastEventType"
        private const val LAST_EVENT_IS_SENT = "org.radarbase.passive.phone.usage.PhoneUsageManager.lastEventIsSent"
        private const val LAST_USER_INTERACTION = "org.radarcns.phone.lastAction"
        private const val ACTION_BOOT = "org.radarcns.phone.ACTION_BOOT"
        private const val ACTION_UPDATE_EVENTS = "org.radarbase.passive.phone.usage.PhoneUsageManager.ACTION_UPDATE_EVENTS"
        private const val USAGE_EVENT_REQUEST_CODE = 586106
    }
}
