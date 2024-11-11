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

package org.radarbase.passive.phone

import android.annotation.SuppressLint
import android.content.Context
import android.content.Context.POWER_SERVICE
import android.content.Intent
import android.content.IntentFilter
import android.hardware.Sensor
import android.hardware.SensorEvent
import android.hardware.SensorEventListener
import android.hardware.SensorManager
import android.os.BatteryManager.BATTERY_STATUS_CHARGING
import android.os.BatteryManager.BATTERY_STATUS_DISCHARGING
import android.os.BatteryManager.BATTERY_STATUS_FULL
import android.os.BatteryManager.BATTERY_STATUS_NOT_CHARGING
import android.os.BatteryManager.BATTERY_STATUS_UNKNOWN
import android.os.BatteryManager.EXTRA_LEVEL
import android.os.BatteryManager.EXTRA_PLUGGED
import android.os.BatteryManager.EXTRA_SCALE
import android.os.BatteryManager.EXTRA_STATUS
import android.os.PowerManager
import android.os.SystemClock
import android.util.SparseArray
import android.util.SparseIntArray
import org.radarbase.android.data.DataCache
import org.radarbase.android.source.AbstractSourceManager
import org.radarbase.android.source.SourceStatusListener
import org.radarbase.android.util.CoroutineTaskExecutor
import org.radarbase.android.util.OfflineProcessor
import org.radarbase.passive.phone.PhoneSensorService.Companion.PHONE_SENSOR_INTERVAL_DEFAULT
import org.radarcns.kafka.ObservationKey
import org.radarcns.passive.phone.BatteryStatus
import org.radarcns.passive.phone.PhoneAcceleration
import org.radarcns.passive.phone.PhoneBatteryLevel
import org.radarcns.passive.phone.PhoneGyroscope
import org.radarcns.passive.phone.PhoneLight
import org.radarcns.passive.phone.PhoneMagneticField
import org.radarcns.passive.phone.PhoneStepCount
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

class PhoneSensorManager(context: PhoneSensorService) : AbstractSourceManager<PhoneSensorService, PhoneState>(context), SensorEventListener {
    private val accelerationTopic: DataCache<ObservationKey, PhoneAcceleration> = createCache("android_phone_acceleration", PhoneAcceleration())
    private val lightTopic: DataCache<ObservationKey, PhoneLight> = createCache("android_phone_light", PhoneLight())
    private val stepCountTopic: DataCache<ObservationKey, PhoneStepCount> = createCache("android_phone_step_count", PhoneStepCount())
    private val gyroscopeTopic: DataCache<ObservationKey, PhoneGyroscope> = createCache("android_phone_gyroscope", PhoneGyroscope())
    private val magneticFieldTopic: DataCache<ObservationKey, PhoneMagneticField> = createCache("android_phone_magnetic_field", PhoneMagneticField())
    private val batteryTopic: DataCache<ObservationKey, PhoneBatteryLevel> = createCache("android_phone_battery_level", PhoneBatteryLevel())

    var sensorDelays: SparseIntArray = SparseIntArray()
        set(value) {
            sensorTaskExecutor.execute {
                if (field.contentsEquals(value)) {
                    return@execute
                }

                field = value
                if (state.status == SourceStatusListener.Status.CONNECTED) {
                    registerSensors()
                }
            }
        }

    private val sensorSendStates = SparseArray<SensorSendState>()

    private val sensorTaskExecutor = CoroutineTaskExecutor(this::class.simpleName!!)

    private val sensorManager: SensorManager? = context.getSystemService(Context.SENSOR_SERVICE) as SensorManager?
    private val batteryProcessor: OfflineProcessor
    private var lastStepCount = -1
    private var wakeLock: PowerManager.WakeLock? = null

    init {
        name = service.getString(R.string.phoneServiceDisplayName)
        batteryProcessor = OfflineProcessor(context) {
            process = listOf(this@PhoneSensorManager::processBatteryStatus)
            requestCode = REQUEST_CODE_PENDING_INTENT
            requestName = ACTIVITY_LAUNCH_WAKE
            wake = true
        }

        status = if (sensorManager == null) {
            SourceStatusListener.Status.UNAVAILABLE
        } else {
            SourceStatusListener.Status.READY
        }
    }

    @SuppressLint("WakelockTimeout")
    override fun start(acceptableIds: Set<String>) {
        register()
        sensorTaskExecutor.start()
        sensorTaskExecutor.execute {
            wakeLock = (service.getSystemService(POWER_SERVICE) as PowerManager?)?.let { pm ->
                pm.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "org.radarcns.phone:PhoneSensorManager")
                    .also { it.acquire() }
            }
            registerSensors()
            status = SourceStatusListener.Status.CONNECTED
        }

        batteryProcessor.start {
            batteryProcessor.trigger()
        }
    }

    fun setBatteryUpdateInterval(period: Long, batteryIntervalUnit: TimeUnit) {
        batteryProcessor.interval(period, batteryIntervalUnit)
    }

    /**
     * Register all sensors supplied in SENSOR_TYPES_TO_REGISTER constant.
     */
    private fun registerSensors() {
        sensorManager ?: return
        sensorTaskExecutor.executeReentrant {
            if (state.status == SourceStatusListener.Status.CONNECTED) {
                sensorManager.unregisterListener(this)
            }

            // At time of writing this is: Accelerometer, Light, Gyroscope, Magnetic Field and Step Counter
            SENSOR_TYPES_TO_REGISTER.forEach { sensorManager.registerSensor(it) }
        }
    }

    private fun SensorManager.registerSensor(sensorType: Int) {
        // delay from milliseconds to microseconds
        val delay = TimeUnit.MILLISECONDS.toMicros(
            sensorDelays.get(sensorType, PHONE_SENSOR_INTERVAL_DEFAULT).toLong()
        ).toInt()
        if (delay <= 0) {
            logger.info("Sensor {} is disabled in configuration", sensorType.toSensorName())
            return
        }

        val sensor = getDefaultSensor(sensorType)
        if (sensor == null) {
            logger.warn("The sensor '{}' could not be found", sensorType.toSensorName())
            return
        }

        val result = registerListener(this@PhoneSensorManager, sensor, delay)
        logger.info(
            "Registered listener for {} sensor at sampling interval {} microseconds: {}",
            sensor.name,
            delay,
            if (result) "succeeded" else "failed",
        )
    }

    override fun onSensorChanged(event: SensorEvent) {
        val time = currentTime
        val sensorType = event.sensor.type
        sensorTaskExecutor.execute {
            val delay = sensorDelays[sensorType]
            // Ignore disabled sensors
            if (delay <= 0) return@execute

            val sendState = sensorSendStates.computeIfAbsent(sensorType) { SensorSendState() }

            if (sendState.mayStartNewInterval(delay)) {
                processSensorEvent(sensorType, time, event, sendState)
            } else {
                sendState.postponeEvent(time, event, sensorTaskExecutor, delay) { postponedTime, postponedEvent ->
                    sensorTaskExecutor.execute {
                        processSensorEvent(sensorType, postponedTime, postponedEvent, sendState)
                    }
                }
            }
        }
    }

    private suspend fun processSensorEvent(
        sensorType: Int,
        time: Double,
        event: SensorEvent,
        sendState: SensorSendState
    ) {
        when (sensorType) {
            Sensor.TYPE_ACCELEROMETER -> processAcceleration(time, event)
            Sensor.TYPE_LIGHT -> processLight(time, event)
            Sensor.TYPE_GYROSCOPE -> processGyroscope(time, event)
            Sensor.TYPE_MAGNETIC_FIELD -> processMagneticField(time, event)
            Sensor.TYPE_STEP_COUNTER -> processStep(time, event)
            else -> logger.debug("Phone registered unknown sensor change: '{}'", event.sensor.type)
        }
        sendState.didProcessEvent()
    }

    override fun onAccuracyChanged(sensor: Sensor, accuracy: Int) {
        // no action
    }

    private suspend fun processAcceleration(time: Double, event: SensorEvent) {
        // x,y,z are in m/s2
        val x = event.values[0] / SensorManager.GRAVITY_EARTH
        val y = event.values[1] / SensorManager.GRAVITY_EARTH
        val z = event.values[2] / SensorManager.GRAVITY_EARTH
        state.setAcceleration(x, y, z)

        send(accelerationTopic, PhoneAcceleration(time, time, x, y, z))
    }

    private suspend fun processLight(time: Double, event: SensorEvent) {
        val lightValue = event.values[0]

        send(lightTopic, PhoneLight(time, time, lightValue))
    }

    private suspend fun processGyroscope(time: Double, event: SensorEvent) {
        // Not normalized axis of rotation in rad/s
        val axisX = event.values[0]
        val axisY = event.values[1]
        val axisZ = event.values[2]

        send(gyroscopeTopic, PhoneGyroscope(time, time, axisX, axisY, axisZ))
    }

    private suspend fun processMagneticField(time: Double, event: SensorEvent) {
        // Magnetic field in microTesla
        val axisX = event.values[0]
        val axisY = event.values[1]
        val axisZ = event.values[2]

        send(magneticFieldTopic, PhoneMagneticField(time, time, axisX, axisY, axisZ))
    }

    private suspend fun processStep(time: Double, event: SensorEvent) {
        // Number of step since listening or since reboot
        val stepCount = event.values[0].toInt()

        // Send how many steps have been taken since the last time this function was triggered
        // Note: normally processStep() is called for every new step and the stepsSinceLastUpdate is 1
        val stepsSinceLastUpdate = if (lastStepCount == -1 || lastStepCount > stepCount) {
            1
        } else {
            stepCount - lastStepCount
        }
        lastStepCount = stepCount
        send(stepCountTopic, PhoneStepCount(time, time, stepsSinceLastUpdate))

        logger.info("Steps taken: {}", stepsSinceLastUpdate)
    }

    private suspend fun processBatteryStatus() {
        // Get last broadcast battery change intent
        val intent = service.registerReceiver(null,
                IntentFilter(Intent.ACTION_BATTERY_CHANGED)) ?: return

        val level = intent.getIntExtra(EXTRA_LEVEL, -1)
        val scale = intent.getIntExtra(EXTRA_SCALE, -1)

        val batteryPct = level / scale.toFloat()

        val isPlugged = intent.getIntExtra(EXTRA_PLUGGED, 0) > 0
        val status = intent.getIntExtra(EXTRA_STATUS, BATTERY_STATUS_UNKNOWN)
        val batteryStatus = status.toBatteryStatus()

        state.batteryLevel = batteryPct

        val time = currentTime
        send(batteryTopic, PhoneBatteryLevel(time, time, batteryPct, isPlugged, batteryStatus))
    }

    override fun onClose() {


        sensorTaskExecutor.stop {
            batteryProcessor.stop()
            sensorManager?.unregisterListener(this)
            wakeLock?.release()
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(PhoneSensorManager::class.java)

        // Sensors to register, together with the name of the sensor
        private val SENSOR_TYPES_TO_REGISTER = intArrayOf(
            Sensor.TYPE_ACCELEROMETER,
            Sensor.TYPE_LIGHT,
            Sensor.TYPE_MAGNETIC_FIELD,
            Sensor.TYPE_GYROSCOPE,
            Sensor.TYPE_STEP_COUNTER,
        )

        private fun Int.toSensorName(): String = when (this) {
            Sensor.TYPE_ACCELEROMETER  -> Sensor.STRING_TYPE_ACCELEROMETER
            Sensor.TYPE_LIGHT          -> Sensor.STRING_TYPE_LIGHT
            Sensor.TYPE_MAGNETIC_FIELD -> Sensor.STRING_TYPE_MAGNETIC_FIELD
            Sensor.TYPE_GYROSCOPE      -> Sensor.STRING_TYPE_GYROSCOPE
            Sensor.TYPE_STEP_COUNTER   -> Sensor.STRING_TYPE_STEP_COUNTER
            else -> "unknown"
        }

        private fun Int.toBatteryStatus(): BatteryStatus = when (this) {
            BATTERY_STATUS_UNKNOWN      -> BatteryStatus.UNKNOWN
            BATTERY_STATUS_CHARGING     -> BatteryStatus.CHARGING
            BATTERY_STATUS_DISCHARGING  -> BatteryStatus.DISCHARGING
            BATTERY_STATUS_NOT_CHARGING -> BatteryStatus.NOT_CHARGING
            BATTERY_STATUS_FULL         -> BatteryStatus.FULL
            else -> BatteryStatus.UNKNOWN
        }

        private const val ACTIVITY_LAUNCH_WAKE = "org.radarbase.passive.phone.PhoneSensorManager.ACTIVITY_LAUNCH_WAKE"
        private const val REQUEST_CODE_PENDING_INTENT = 482480668

        private fun SparseIntArray.contentsEquals(other: SparseIntArray): Boolean {
            return size() == other.size() &&
                    (0 until size())
                        .all { keyAt(it) == other.keyAt(it) && valueAt(it) == other.valueAt(it) }
        }

        private inline fun <T> SparseArray<T>.computeIfAbsent(key: Int, compute: () -> T) = get(key)
            ?: compute().also { put(key, it) }

        data class SensorSendState(
            var lastSendTime: Long = 0,
            var postponedTime: Double? = null,
            var postponedEvent: SensorEvent? = null,
            var postponeFuture: CoroutineTaskExecutor.CoroutineFutureHandle? = null,
        ) {
            /**
             * Whether the previous interval has ended. The interval is measured as [delay]
             * milliseconds. The interval starts when the first event in an interval is processed.
             */
            fun mayStartNewInterval(delay: Int): Boolean = lastSendTime + delay <= now
            /**
             * Time from now when not only the current but also the next interval has ended.
             * The interval is measured as [delay] milliseconds.
             */
            private fun timeUntilNextIntervalEnds(delay: Int): Long = lastSendTime + 2 * delay - now

            /**
             * Call when an event is processed. This marks the start of an interval and will discard
             * any postponed events.
             */
            fun didProcessEvent() {
                lastSendTime = now
                postponedTime = null
                postponedEvent = null
                postponeFuture?.let {
                    it.cancel()
                    postponeFuture = null
                }
            }

            /**
             * Postpone sending [event] until the next interval ends. Only the last event
             * in the current interval will be sent and only if no events are sent in the interval
             * after it. This method may be called multiple times in an interval. The postponed
             * action will be run by [CoroutineTaskExecutor]. The interval is measured as [delay] milliseconds.
             */
            fun postponeEvent(
                time: Double,
                event: SensorEvent,
                executor: CoroutineTaskExecutor,
                delay: Int,
                process: (postponedTime: Double, postponedEvent: SensorEvent) -> Unit,
            ) {
                postponedTime = time
                postponedEvent = event
                if (postponeFuture == null) {
                    postponeFuture = executor.delay(timeUntilNextIntervalEnds(delay)) {
                        postponeFuture = null
                        process(
                            postponedTime ?: return@delay,
                            postponedEvent ?: return@delay,
                        )
                    }
                }
            }

            companion object {
                private val now: Long
                    get() = SystemClock.uptimeMillis()
            }
        }
    }
}
