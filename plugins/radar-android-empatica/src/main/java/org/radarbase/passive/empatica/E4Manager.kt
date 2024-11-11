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

package org.radarbase.passive.empatica

import com.empatica.empalink.ConnectionNotAllowedException
import com.empatica.empalink.EmpaDeviceManager
import com.empatica.empalink.EmpaticaDevice
import com.empatica.empalink.config.EmpaSensorStatus
import com.empatica.empalink.config.EmpaSensorType
import com.empatica.empalink.config.EmpaSessionEvent
import com.empatica.empalink.config.EmpaStatus
import com.empatica.empalink.delegate.EmpaDataDelegate
import com.empatica.empalink.delegate.EmpaSessionManagerDelegate
import com.empatica.empalink.delegate.EmpaStatusDelegate
import org.radarbase.android.source.AbstractSourceManager
import org.radarbase.android.source.SourceStatusListener
import org.radarbase.android.util.BluetoothStateReceiver.Companion.bluetoothIsEnabled
import org.radarbase.android.util.CoroutineTaskExecutor
import org.radarbase.android.util.NotificationHandler
import org.radarbase.android.util.SafeHandler
import org.radarcns.passive.empatica.*
import org.slf4j.LoggerFactory
import java.lang.Exception
import java.util.concurrent.atomic.AtomicBoolean

/** Manages scanning for an Empatica E4 wearable and connecting to it  */
class E4Manager(
        e4Service: E4Service,
        private val empaManager: EmpaDeviceManager,
        private val handler: SafeHandler
) : AbstractSourceManager<E4Service, E4State>(e4Service),
        EmpaDataDelegate,
        EmpaStatusDelegate,
        EmpaSessionManagerDelegate {
    private val notificationHandler: NotificationHandler by lazy {
        NotificationHandler(service)
    }
    private var doNotify: Boolean = false
    private val accelerationTopic = createCache("android_empatica_e4_acceleration", EmpaticaE4Acceleration())
    private val batteryLevelTopic = createCache("android_empatica_e4_battery_level", EmpaticaE4BatteryLevel())
    private val bloodVolumePulseTopic = createCache("android_empatica_e4_blood_volume_pulse", EmpaticaE4BloodVolumePulse())
    private val edaTopic = createCache("android_empatica_e4_electrodermal_activity", EmpaticaE4ElectroDermalActivity())
    private val interBeatIntervalTopic = createCache("android_empatica_e4_inter_beat_interval", EmpaticaE4InterBeatInterval())
    private val temperatureTopic = createCache("android_empatica_e4_temperature", EmpaticaE4Temperature())
    private val sensorStatusTopic = createCache("android_empatica_e4_sensor_status", EmpaticaE4SensorStatus())
    private val tagTopic = createCache("android_empatica_e4_tag", EmpaticaE4Tag())

    private val isScanning = AtomicBoolean(false)
    private var hasBeenConnecting = false
    private var apiKey: String? = null
    private var connectingFuture: SafeHandler.HandlerFuture? = null

    private val dataSenderExecutor: CoroutineTaskExecutor = CoroutineTaskExecutor(this::class.simpleName!!)

    init {
        status = SourceStatusListener.Status.UNAVAILABLE
        name = service.getString(R.string.empaticaE4DisplayName)
    }

    override fun start(acceptableIds: Set<String>) {
        logger.info("Starting scanning")
        handler.execute {
            // Create a new EmpaDeviceManager. E4DeviceManager is both its data and status delegate.
            // Initialize the Device Manager using your API key. You need to have Internet access at this point.
            logger.info("Authenticating EmpaDeviceManager")
            try {
                empaManager.authenticateWithAPIKey(apiKey)
            } catch (ex: Throwable) {
                if (ex.message == "Status is not INITIAL") {
                    logger.info("Already authenticated with Empatica")
                } else {
                    logger.error("Failed to authenticate with Empatica", ex)
                    disconnect()
                }
            }
            logger.info("Authenticated EmpaDeviceManager")
        }

        // Restart scanning after a fixed timeout, to prevent BLE from stopping scanning after 30mins (on Android N)
        // https://github.com/AltBeacon/android-beacon-library/pull/529
        handler.repeat(SCAN_TIMEOUT) {
            if (isScanning.get()) {
                // start scanning again
                try {
                    logger.info("Stopping scanning (BLE timeout)")
                    empaManager.stopScanning()
                    logger.info("Starting scanning (BLE timeout)")
                    empaManager.startScanning()
                    logger.info("Started scanning (BLE timeout)")
                } catch (ex: RuntimeException) {
                    logger.error("Empatica internally did not initialize")
                }
            }
        }
    }

    override fun didUpdateStatus(empaStatus: EmpaStatus) {
        logger.info("{}: Updated E4 status to {}", hashCode(), empaStatus)
        when (empaStatus) {
            EmpaStatus.READY -> handler.execute {
                // The device manager is ready for use
                // Start scanning
                try {
                    if (isScanning.compareAndSet(false, true)) {
                        empaManager.startScanning()
                        logger.info("Started scanning")
                    }
                    status = SourceStatusListener.Status.READY
                } catch (ex: RuntimeException) {
                    logger.error("Empatica internally did not initialize")
                    disconnect()
                }
            }
            EmpaStatus.CONNECTING -> hasBeenConnecting = true
            EmpaStatus.CONNECTED -> {
                status = SourceStatusListener.Status.CONNECTED
                notificationHandler.manager?.cancel(EMPATICA_DISCONNECTED_NOTIFICATION_ID)
            }
            EmpaStatus.DISCONNECTED ->
                // The device manager disconnected from a device. Before it ever makes a connection,
                // it also calls this, so check if we have a connected device first.
                if (hasBeenConnecting) {
                    disconnect()
                }
            else -> {
                // not handling all cases
            }
        }
    }

    override fun bluetoothStateChanged() {
        logger.info("E4 bluetooth state has changed.")
    }

    override fun didEstablishConnection() {
        logger.info("Established connection with E4")
    }

    override fun didDiscoverDevice(empaDevice: EmpaticaDevice, deviceName: String, rssi: Int, allowed: Boolean) {
        // Check if the discovered device can be used with your API key. If allowed is always false,
        // the device is not linked with your API key. Please check your developer area at
        // https://www.empatica.com/connect/developer.php
        val address = empaDevice.device.address
        logger.info("{}: Bluetooth address: {}", System.identityHashCode(this), address)
        if (allowed) {
            register(
                    name = deviceName,
                    physicalId = empaDevice.hardwareId,
                    attributes = mapOf(
                            Pair("sdk", "empalink-2.2.aar"),
                            Pair("macAddress", address),
                            Pair("serialNumber", empaDevice.serialNumber))) {
                if (it == null) {
                    logger.info("Device {} with ID {} is not listed in acceptable device IDs",
                        deviceName, address)
                    service.sourceFailedToConnect(deviceName)
                } else {
                    handler.execute {
                        if (connectingFuture != null) {
                            return@execute
                        }
                        name = service.getString(R.string.e4DeviceName, deviceName)
                        stopScanning()
                        connectingFuture = handler.delay(500L) {
                            connectingFuture = null
                            logger.info("Will connect device {}", deviceName)
                            try {
                                // Connect to the device
                                status = SourceStatusListener.Status.CONNECTING
                                empaManager.connectDevice(empaDevice)
                            } catch (e: ConnectionNotAllowedException) {
                                // This should happen only if you try to connect when allowed == false.
                                service.sourceFailedToConnect(deviceName)
                            } catch (e: Exception) {
                                logger.error("E4 device failed to connect", e)
                                disconnect()
                            }
                        }
                    }
                }
            }
        } else {
            logger.warn("Device {} with address {} is not an allowed device.", deviceName, empaDevice.device.address)
            service.sourceFailedToConnect(deviceName)
        }
    }

    private fun stopScanning() {
        if (isScanning.compareAndSet(true, false)) {
            try {
                empaManager.stopScanning()
            } catch (ex: NullPointerException) {
                logger.warn("Empatica internally already stopped scanning")
            }
        }
    }

    override fun onClose() {
        handler.execute(true) {
            stopScanning()
            connectingFuture?.let {
                it.cancel()
                connectingFuture = null
            }
            logger.info("Initiated device {} stop-sequence", name)
            if (hasBeenConnecting) {
                try {
                    empaManager.cancelConnection()
                } catch (ex: Exception) {
                    logger.warn("Empatica internally already disconnected")
                }
            }
            logger.info("Finished device {} stop-sequence", name)
        }
    }

    override fun didRequestEnableBluetooth() {
        if (!service.bluetoothIsEnabled) {
            logger.warn("Bluetooth is not enabled.")
            disconnect()
        }
    }

    override fun disconnect() {
        if (status != SourceStatusListener.Status.UNAVAILABLE && !isClosed && doNotify) {
            notificationHandler.notify(
                id = EMPATICA_DISCONNECTED_NOTIFICATION_ID,
                channel = NotificationHandler.NOTIFICATION_CHANNEL_ALERT,
                includeStartIntent = true,
            ) {
                setContentTitle(service.getString(R.string.notification_empatica_disconnected_title))
                setContentText(service.getString(R.string.notification_empatica_disconnected_text))
            }
        }

        super.disconnect()
    }

    override fun didUpdateOnWristStatus(status: Int) {
        val now = currentTime
        dataSenderExecutor.execute {
            send(
                sensorStatusTopic, EmpaticaE4SensorStatus(
                    now, now, "e4", status.toEmpaStatusString()
                )
            )
        }
    }

    override fun didReceiveAcceleration(x: Int, y: Int, z: Int, timestamp: Double) {
        state.setAcceleration(x / 64f, y / 64f, z / 64f)
        val latestAcceleration = state.acceleration
        dataSenderExecutor.execute {
            send(
                accelerationTopic, EmpaticaE4Acceleration(
                    timestamp, currentTime,
                    latestAcceleration[0], latestAcceleration[1], latestAcceleration[2]
                )
            )
        }
    }

    override fun didReceiveBVP(bvp: Float, timestamp: Double) {
        state.bloodVolumePulse = bvp
        dataSenderExecutor.execute {
            send(
                bloodVolumePulseTopic, EmpaticaE4BloodVolumePulse(
                    timestamp, currentTime, bvp
                )
            )
        }
    }

    override fun didReceiveBatteryLevel(battery: Float, timestamp: Double) {
        state.batteryLevel = battery
        dataSenderExecutor.execute {
            send(
                batteryLevelTopic, EmpaticaE4BatteryLevel(
                    timestamp, currentTime, battery
                )
            )
        }
        }

    override fun didReceiveTag(timestamp: Double) {
        val value =  EmpaticaE4Tag(timestamp, currentTime)
        dataSenderExecutor.execute {
            send(tagTopic, value)
        }
    }

    override fun didReceiveGSR(gsr: Float, timestamp: Double) {
        state.electroDermalActivity = gsr
        val value = EmpaticaE4ElectroDermalActivity(timestamp, currentTime, gsr)
        dataSenderExecutor.execute {
            send(edaTopic, value)
        }
    }

    override fun didReceiveIBI(ibi: Float, timestamp: Double) {
        state.interBeatInterval = ibi
        val value = EmpaticaE4InterBeatInterval(timestamp, currentTime, ibi)
        dataSenderExecutor.execute {
            send(interBeatIntervalTopic, value)
        }
    }

    override fun didReceiveTemperature(temperature: Float, timestamp: Double) {
        state.temperature = temperature
        val value = EmpaticaE4Temperature(timestamp, currentTime, temperature)
        dataSenderExecutor.execute {
            send(temperatureTopic, value)
        }
    }

    override fun didUpdateSensorStatus(status: Int, type: EmpaSensorType) {
        val statusString = status.toEmpaStatusString()
        state.setSensorStatus(type, statusString)
        val now = currentTime
        val value = EmpaticaE4SensorStatus(now, now, type.name, statusString)
        dataSenderExecutor.execute {
            send(sensorStatusTopic, value)
        }
    }

    override fun hashCode() = System.identityHashCode(this)

    override fun equals(other: Any?) = this === other

    override fun didFailedScanning(errorCode: Int) {
        logger.warn("Failed scanning device: error code {}", errorCode)
    }

    override fun didUpdateSessionStatus(event: EmpaSessionEvent?, progress: Float) {
        logger.info("Empatica session event {} with progress {}", event, progress)
        if (event == EmpaSessionEvent.UNAUTHORIZED_USER_ERROR) {
            disconnect()
        }
    }

    fun notifyDisconnect(doNotify: Boolean) {
        this.doNotify = doNotify
    }

    fun updateApiKey(key: String?) {
        if (key != null && apiKey == null) {
            apiKey = key
            status = SourceStatusListener.Status.READY
        } else if (key != apiKey) {
            disconnect()  // API key changed or got removed
        }
    }

    fun startDisconnect() {
        logger.info("Disconnecting E4 service")
        disconnect()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(E4Manager::class.java)

        private fun Int.toEmpaStatusString(): String = when(this) {
            EmpaSensorStatus.ON_WRIST -> "ON_WRIST"
            EmpaSensorStatus.NOT_ON_WRIST -> "NOT_ON_WRIST"
            EmpaSensorStatus.DEAD -> "DEAD"
            else -> "UNKNOWN"
        }

        // BLE scan timeout
        private const val ANDROID_N_MAX_SCAN_DURATION_MS = 30 * 60 * 1000L // 30 minutes
        private const val SCAN_TIMEOUT = ANDROID_N_MAX_SCAN_DURATION_MS / 2

        private const val EMPATICA_DISCONNECTED_NOTIFICATION_ID = 27286
    }
}
