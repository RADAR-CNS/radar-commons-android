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

package org.radarbase.android.util

import android.annotation.SuppressLint
import android.app.AlarmManager
import android.app.PendingIntent
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Context.ALARM_SERVICE
import android.content.Context.POWER_SERVICE
import android.content.Intent
import android.content.IntentFilter
import android.os.Debug
import android.os.PowerManager
import android.os.SystemClock
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import org.radarbase.kotlin.coroutines.forkJoin
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext

/**
 * Process events based on a alarm. The events will be processed in a background Thread.
 * During processing in the provided Runnable,
 * check that [.isDone] remains `false`. Once it turns true, the Runnable should stop
 * processing. If wake is set to true, [android.Manifest.permission.WAKE_LOCK] should be
 * acquired in the Manifest.
 */
class OfflineProcessor(
    private val context: Context,
    config: ProcessorConfiguration,
) {

    constructor(
        context: Context,
        config: ProcessorConfiguration.() -> Unit,
    ) : this(context, ProcessorConfiguration().apply(config))

    private val config = config.copy()
    private val receiver: BroadcastReceiver
    private val pendingIntent: PendingIntent
    private val alarmManager: AlarmManager
    private val job = SupervisorJob()
    private val processorScope = CoroutineScope(config.coroutineContext + job)

    private val requestName: String = requireNotNull(config.requestName) { "Cannot start processor without request name" }
    private val process: List<suspend () -> Unit> = ArrayList(config.process)

    private val _isDone = AtomicBoolean(false)

    /** Whether the processing Runnable should stop execution.  */
    val isDone: Boolean
        get() = _isDone.get()

    @Volatile
    var isStarted: Boolean = false
        private set

    private val runningLock: Mutex

    init {
        require(process.isNotEmpty()) { "Cannot start processor without processes" }
        this.alarmManager = context.getSystemService(ALARM_SERVICE) as AlarmManager

        val intent = Intent(config.requestName)
        pendingIntent = PendingIntent.getBroadcast(
            context,
            requireNotNull(config.requestCode) { "Cannot start processor without request code" },
            intent,
            PendingIntent.FLAG_UPDATE_CURRENT.toPendingIntentFlag(),
        )

        this.receiver = object : BroadcastReceiver() {
            override fun onReceive(context: Context, intent: Intent) {
                trigger()
            }
        }
        runningLock = Mutex()
    }

    /** Start processing.  */
    fun start(initializer: (suspend () -> Unit)? = null) {
        processorScope.launch {
            isStarted = true
            context.registerReceiver(receiver, IntentFilter(requestName))
            schedule()
            if (initializer != null) {
                initializer()
            }
        }
    }

    /** Start up a new thread to process.  */
    fun trigger() {
        processorScope.launch {
            runningLock.tryWithLock {
                val wakeLock = if (config.wake) {
                    acquireWakeLock(context, requestName)
                } else null

                try {
                    process.forkJoin { runnable ->
                        try {
                            runnable()
                        } catch (ex: Throwable) {
                            logger.error("OfflineProcessor task failed.", ex)
                        }
                    }
                } catch (ex: RuntimeException) {
                    logger.error("Handler thread is no longer running.", ex)
                } finally {
                    wakeLock?.release()
                }
            }
        }
    }

    /**
     * Change the processing interval to the given value.
     * @param duration time between processing.
     * @param timeUnit time unit to that duration is given with
     */
    fun interval(duration: Long, timeUnit: TimeUnit) {
        require(duration > 0L) { "Duration must be positive" }
        processorScope.launch {
            runningLock.withLock {
                if (config.interval(duration, timeUnit) && isStarted) {
                    schedule()
                }
            }
        }
    }

    private suspend fun schedule() {
        val runImmediately = Debug.isDebuggerConnected()
        val firstAlarm: Long = if (runImmediately) {
            trigger()
            SystemClock.elapsedRealtime() + config.intervalMillis
        } else {
            SystemClock.elapsedRealtime() + config.intervalMillis / 4
        }
        val type = if (config.wake) AlarmManager.ELAPSED_REALTIME_WAKEUP else AlarmManager.ELAPSED_REALTIME
        withContext(Dispatchers.IO) {
            alarmManager.setInexactRepeating(type, firstAlarm, config.intervalMillis, pendingIntent)
        }
    }

    /**
     * Closes the processor.
     *
     * This will deregister any BroadcastReceiver, remove pending alarms and signal the running thread to stop. If
     * processing is currently taking place, it will block until that is actually done.
     * The processing Runnable should query [.isDone] very regularly to stop execution
     * if that is the case.
     */
    suspend fun stop() {
        if (!_isDone.compareAndSet(false, true)) {
            logger.info("OfflineProcessor attempted to be closed twice.")
            return
        }
        job.cancelAndJoin()

        if (isStarted) {
            coroutineScope {
                launch(Dispatchers.IO) {
                    alarmManager.cancel(pendingIntent)
                }
                launch(Dispatchers.IO) {
                    try {
                        context.unregisterReceiver(receiver)
                    } catch (ex: IllegalStateException) {
                        logger.warn(
                            "Cannot unregister OfflineProcessor {}, it was most likely not completely started: {}",
                            config.requestName,
                            ex.message,
                        )
                    }
                }
            }
        }
    }

    data class ProcessorConfiguration(
        var requestCode: Int? = null,
        var requestName: String? = null,
        var process: List<suspend () -> Unit> = emptyList(),
        var intervalMillis: Long = -1,
        var wake: Boolean = true,
        var coroutineContext: CoroutineContext = EmptyCoroutineContext,
    ) {
        @Synchronized
        fun interval(duration: Long, unit: TimeUnit): Boolean {
            require(duration > 0L) { "Duration must be positive" }
            val oldInterval = intervalMillis
            intervalMillis = unit.toMillis(duration)
            return oldInterval != intervalMillis
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(OfflineProcessor::class.java)

        @SuppressLint("WakelockTimeout")
        private fun acquireWakeLock(
            context: Context,
            requestName: String?,
        ): PowerManager.WakeLock? {
            val powerManager = context.getSystemService(POWER_SERVICE) as PowerManager? ?: return null
            val lock = powerManager.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, requestName)
            lock.acquire()
            return lock
        }

        private suspend fun <T> Mutex.tryWithLock(block: suspend () -> T): T? {
            if (!tryLock()) {
                return null
            }
            return try {
                block()
            } finally {
                unlock()
            }
        }
    }
}
