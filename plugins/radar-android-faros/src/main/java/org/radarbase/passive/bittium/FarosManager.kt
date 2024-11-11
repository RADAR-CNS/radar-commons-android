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

package org.radarbase.passive.bittium

import org.radarbase.android.data.DataCache
import org.radarbase.android.source.AbstractSourceManager
import org.radarbase.android.source.SourceStatusListener
import org.radarbase.android.util.CoroutineTaskExecutor
import org.radarbase.android.util.NotificationHandler
import org.radarbase.android.util.SafeHandler
import org.radarbase.util.Strings
import org.radarcns.bittium.faros.*
import org.radarcns.kafka.ObservationKey
import org.radarcns.passive.bittium.*
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.regex.Pattern

class FarosManager internal constructor(
        service: FarosService,
        private val farosFactory: FarosSdkFactory,
        private val handler: SafeHandler
) : AbstractSourceManager<FarosService, FarosState>(service), FarosDeviceListener, FarosSdkListener {
    private val notificationHandler by lazy {
        NotificationHandler(this.service)
    }
    private var doNotify: Boolean = false
    private val accelerationTopic: DataCache<ObservationKey, BittiumFarosAcceleration> = createCache("android_bittium_faros_acceleration", BittiumFarosAcceleration())
    private val ecgTopic: DataCache<ObservationKey, BittiumFarosEcg> = createCache("android_bittium_faros_ecg", BittiumFarosEcg())
    private val ibiTopic: DataCache<ObservationKey, BittiumFarosInterBeatInterval> = createCache("android_bittium_faros_inter_beat_interval", BittiumFarosInterBeatInterval())
    private val temperatureTopic: DataCache<ObservationKey, BittiumFarosTemperature> = createCache("android_bittium_faros_temperature", BittiumFarosTemperature())
    private val batteryTopic: DataCache<ObservationKey, BittiumFarosBatteryLevel> = createCache("android_bittium_faros_battery_level", BittiumFarosBatteryLevel())

    private lateinit var acceptableIds: Array<Pattern>
    private lateinit var apiManager: FarosSdkManager
    private var settings: FarosSettings = farosFactory.defaultSettingsBuilder().build()

    private val farosTaskExecutor: CoroutineTaskExecutor = CoroutineTaskExecutor(this::class.simpleName!!)

    private var faros: FarosDevice? = null

    override fun start(acceptableIds: Set<String>) {
        logger.info("Faros searching for device.")
        service.getString(R.string.farosLabel)

        handler.start()
        farosTaskExecutor.start()
        farosTaskExecutor.execute {
            this.acceptableIds = Strings.containsPatterns(acceptableIds)

            apiManager = farosFactory.createSdkManager(service)
            try {
                apiManager.startScanning(this, handler.handler)
                status = SourceStatusListener.Status.READY
            } catch (ex: IllegalStateException) {
                logger.error("Failed to start scanning", ex)
                close()
            }
        }
    }

    override fun onStatusUpdate(status: Int) {
        val radarStatus = when(status) {
            FarosDeviceListener.IDLE -> {
                farosTaskExecutor.execute {
                    logger.debug("Faros status is IDLE. Request to start/restart measurements.")
                    applySettings(this.settings)
                    faros?.run {
                        requestBatteryLevel()
                        startMeasurements()
                    }
                }
                notificationHandler.manager?.cancel(FAROS_DISCONNECTED_NOTIFICATION_ID)
                SourceStatusListener.Status.CONNECTED
            }
            FarosDeviceListener.CONNECTING -> SourceStatusListener.Status.CONNECTING
            FarosDeviceListener.DISCONNECTED, FarosDeviceListener.DISCONNECTING -> {
                disconnect()
                SourceStatusListener.Status.DISCONNECTING
            }
            FarosDeviceListener.MEASURING -> SourceStatusListener.Status.CONNECTED
            else -> {
                logger.warn("Faros status {} is unknown", status)
                return
            }
        }
        logger.debug("Faros status {} and radarStatus {}", status, radarStatus)

        this.status = radarStatus
    }

    override fun onDeviceScanned(device: FarosDevice) {
            logger.info("Found Faros device {}", device.name)
            if (faros != null) {
                logger.info("Faros device {} already set", device.name)
                return
            }

            val attributes: Map<String, String> = mapOf(
                    Pair("type", when(device.type) {
                        FarosDevice.FAROS_90  -> "FAROS_90"
                        FarosDevice.FAROS_180 -> "FAROS_180"
                        FarosDevice.FAROS_360 -> "FAROS_360"
                        else -> "unknown"
                    }))

            if ((acceptableIds.isEmpty() || Strings.findAny(acceptableIds, device.name))) {
                register(device.name, device.name, attributes) {
                    if (it != null) {
                        farosTaskExecutor.executeReentrant {
                            logger.info("Stopping scanning")
                            apiManager.stopScanning()
                            name = service.getString(R.string.farosDeviceName, device.name)

                            logger.info("Connecting to device {}", device.name)
                            device.connect(this, handler.handler)
                            this.faros = device
                            status = SourceStatusListener.Status.CONNECTING
                        }
                    } else {
                        logger.warn("Cannot register a Faros device")
                    }
                }
        }
    }

    override fun didReceiveAcceleration(timestamp: Double, x: Float, y: Float, z: Float) {
        state.setAcceleration(x, y, z)
        farosTaskExecutor.execute {
            send(accelerationTopic, BittiumFarosAcceleration(timestamp, currentTime, x, y, z))
        }
    }

    override fun didReceiveTemperature(timestamp: Double, temperature: Float) {
        state.temperature = temperature
        farosTaskExecutor.execute {
            send(temperatureTopic, BittiumFarosTemperature(timestamp, currentTime, temperature))
        }
    }

    override fun didReceiveInterBeatInterval(timestamp: Double, interBeatInterval: Float) {
        state.heartRate = 60 / interBeatInterval
        farosTaskExecutor.execute {
            send(ibiTopic, BittiumFarosInterBeatInterval(timestamp, currentTime, interBeatInterval))
        }
    }

    override fun didReceiveEcg(timestamp: Double, channels: FloatArray) {
        val channelOne = channels[0]
        val channelTwo = if (channels.size > 1) channels[1] else null
        val channelThree = if (channels.size > 2) channels[2] else null

        farosTaskExecutor.execute {
            send(
                ecgTopic,
                BittiumFarosEcg(timestamp, currentTime, channelOne, channelTwo, channelThree)
            )
        }
    }

    override fun didReceiveBatteryStatus(timestamp: Double, status: Int) {
        // only send approximate battery levels if the battery level interval is disabled.
        val level = when (status) {
            FarosDeviceListener.BATTERY_STATUS_CRITICAL -> 0.05f
            FarosDeviceListener.BATTERY_STATUS_LOW -> 0.175f
            FarosDeviceListener.BATTERY_STATUS_MEDIUM -> 0.5f
            FarosDeviceListener.BATTERY_STATUS_FULL -> 0.875f
            else -> {
                logger.warn("Unknown battery status {} passed", status)
                return
            }
        }
        state.batteryLevel = level
        farosTaskExecutor.execute {
            send(batteryTopic, BittiumFarosBatteryLevel(timestamp, currentTime, level, false))
        }
    }

    override fun didReceiveBatteryLevel(timestamp: Double, level: Float) {
        state.batteryLevel = level
        farosTaskExecutor.execute {
            send(batteryTopic, BittiumFarosBatteryLevel(timestamp, currentTime, level, true))
        }
    }

    internal fun applySettings(settings: FarosSettings) {
        farosTaskExecutor.executeReentrant {
            this.settings = settings
            faros?.run {
                if (isMeasuring) {
                    logger.info("Device is measuring. Stopping device before applying settings.")
                    stopMeasurements()
                    // will apply in onStatusUpdate(), when the device becomes idle.
                } else {
                    logger.info("Applying device settings {}", settings)
                    apply(settings)
                }
            }
        }
    }

    override fun disconnect() {
        if (!isClosed && doNotify) {
            notificationHandler.notify(
                id = FAROS_DISCONNECTED_NOTIFICATION_ID,
                channel = NotificationHandler.NOTIFICATION_CHANNEL_ALERT,
                includeStartIntent = true,
            ) {
                setContentTitle(service.getString(R.string.notification_faros_disconnected_title))
                setContentText(service.getString(R.string.notification_faros_disconnected_text))
            }
        }

        super.disconnect()
    }

    override fun onClose() {
        logger.info("Faros BT Closing device {}", this)

        handler.stop()
        farosTaskExecutor.stop {
            try {
                faros?.close()
            } catch (e2: IOException) {
                logger.error("Faros socket close failed")
            } catch (npe: NullPointerException) {
                logger.info("Can't close an unopened socket")
            }

            apiManager.close()
        }
    }

    fun notifyDisconnect(doNotify: Boolean) {
        this.doNotify = doNotify
    }

    companion object {
        private val logger = LoggerFactory.getLogger(FarosManager::class.java)

        private const val FAROS_DISCONNECTED_NOTIFICATION_ID = 27287
    }
}
