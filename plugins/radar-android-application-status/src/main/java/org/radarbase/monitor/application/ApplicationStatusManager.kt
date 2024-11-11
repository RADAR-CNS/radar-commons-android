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

package org.radarbase.monitor.application

import android.content.Context
import android.content.SharedPreferences
import android.content.pm.PackageManager
import android.os.Build
import android.os.SystemClock
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.radarbase.android.data.DataCache
import org.radarbase.android.data.TableDataHandler
import org.radarbase.android.kafka.TopicSendReceipt
import org.radarbase.android.source.AbstractSourceManager
import org.radarbase.android.source.SourceStatusListener
import org.radarbase.android.util.BroadcastRegistration
import org.radarbase.android.util.ChangeRunner
import org.radarbase.android.util.CoroutineTaskExecutor
import org.radarbase.android.util.OfflineProcessor
import org.radarbase.android.util.takeTrimmedIfNotEmpty
import org.radarbase.monitor.application.ApplicationStatusService.Companion.UPDATE_RATE_DEFAULT
import org.radarcns.kafka.ObservationKey
import org.radarcns.monitor.application.ApplicationDeviceInfo
import org.radarcns.monitor.application.ApplicationExternalTime
import org.radarcns.monitor.application.ApplicationRecordCounts
import org.radarcns.monitor.application.ApplicationServerStatus
import org.radarcns.monitor.application.ApplicationTimeZone
import org.radarcns.monitor.application.ApplicationUptime
import org.radarcns.monitor.application.ExternalTimeProtocol
import org.radarcns.monitor.application.OperatingSystem
import org.radarcns.monitor.application.ServerStatus
import org.slf4j.LoggerFactory
import java.net.InetAddress
import java.net.NetworkInterface
import java.net.SocketException
import java.util.TimeZone
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.SECONDS

class ApplicationStatusManager(
    service: ApplicationStatusService
) : AbstractSourceManager<ApplicationStatusService, ApplicationState>(service) {
    private val serverTopic: DataCache<ObservationKey, ApplicationServerStatus> = createCache("application_server_status", ApplicationServerStatus())
    private val recordCountsTopic: DataCache<ObservationKey, ApplicationRecordCounts> = createCache("application_record_counts", ApplicationRecordCounts())
    private val uptimeTopic: DataCache<ObservationKey, ApplicationUptime> = createCache("application_uptime", ApplicationUptime())
    private val ntpTopic: DataCache<ObservationKey, ApplicationExternalTime> = createCache("application_external_time", ApplicationExternalTime())
    private val timeZoneTopic: DataCache<ObservationKey, ApplicationTimeZone> = createCache("application_time_zone", ApplicationTimeZone())
    private val deviceInfoTopic: DataCache<ObservationKey, ApplicationDeviceInfo> = createCache("application_device_info", ApplicationDeviceInfo())

    private val processor: OfflineProcessor
    private val creationTimeStamp: Long = SystemClock.elapsedRealtime()
    private val sntpClient: SntpClient = SntpClient()
    private val prefs: SharedPreferences = service.getSharedPreferences(ApplicationStatusManager::class.java.name, Context.MODE_PRIVATE)
    private var tzProcessor: OfflineProcessor? = null

    private val applicationStatusExecutor: CoroutineTaskExecutor =
        CoroutineTaskExecutor(this::class.simpleName!!)

    @get:Synchronized
    @set:Synchronized
    var isProcessingIp: Boolean = false

    @get:Synchronized
    @set:Synchronized
    var ntpServer: String? = null
        set(value) {
            field = value?.takeTrimmedIfNotEmpty()
        }

    private var previousInetAddress: InetAddress? = null

    private lateinit var tzOffsetCache: ChangeRunner<Int>
    private lateinit var deviceInfoCache: ChangeRunner<ApplicationInfo>
    private var serverStatusReceiver: BroadcastRegistration? = null
    private var serverRecordsReceiver: BroadcastRegistration? = null
    private var cacheReceiver: BroadcastRegistration? = null

    init {
        name = service.getString(R.string.applicationServiceDisplayName)
        this.processor = OfflineProcessor(service) {
            process = listOf(
                ::processServerStatus,
                ::processUptime,
                ::processRecordsSent,
                ::processReferenceTime,
                ::processDeviceInfo,
            )
            requestCode = APPLICATION_PROCESSOR_REQUEST_CODE
            requestName = APPLICATION_PROCESSOR_REQUEST_NAME
            interval(UPDATE_RATE_DEFAULT, SECONDS)
            wake = false
        }
    }

    override fun start(acceptableIds: Set<String>) {
        status = SourceStatusListener.Status.READY

        applicationStatusExecutor.start()
        processor.start {
            val osVersionCode: Int = this.prefs.getInt("operatingSystemVersionCode", -1)
            val appVersionCode: Int = this.prefs.getInt("appVersionCode", -1)

            deviceInfoCache = ChangeRunner(
                ApplicationInfo(
                    manufacturer = this.prefs.getString("manufacturer", null),
                    model = this.prefs.getString("model", null),
                    osVersion = this.prefs.getString("operatingSystemVersion", null),
                    osVersionCode = if (osVersionCode > 0) osVersionCode else null,
                    appVersion = this.prefs.getString("appVersion", null),
                    appVersionCode = if (appVersionCode > 0) appVersionCode else null,
                ),
            )

            val deviceInfo = currentApplicationInfo

            register(
                name = "pRMT",
                attributes = mapOf(
                    "manufacturer" to (deviceInfo.manufacturer ?: ""),
                    "model" to (deviceInfo.model ?: ""),
                    "operatingSystem" to OperatingSystem.ANDROID.toString(),
                    "operatingSystemVersion" to (deviceInfo.osVersion ?: ""),
                    "appVersion" to (deviceInfo.appVersion ?: ""),
                    "appVersionCode" to (deviceInfo.appVersionCode?.toString() ?: ""),
                    "appName" to service.application.packageName,
                )
            )

            this.tzOffsetCache = ChangeRunner(this.prefs.getInt("timeZoneOffset", -1))
        }
        tzProcessor?.start()

        logger.info("Starting ApplicationStatusManager")
            with(applicationStatusExecutor) {
                service.dataHandler?.let { handler ->
                    execute {
                        handler.numberOfRecords
                            .collect { records: TableDataHandler.CacheSize ->
                                val topic = records.topicName
                                val noOfRecords = records.numberOfRecords
                                logger.trace("Topic {} has {} records in cache", topic, noOfRecords)
                                state.cachedRecords[topic] = noOfRecords.coerceAtLeast(0)
                            }
                        }

                    execute {
                        handler.recordsSent.collect { sent: TopicSendReceipt ->
                            logger.trace("Topic {} sent {} records", sent.topic, sent.numberOfRecords)
                            state.addRecordsSent(sent.numberOfRecords)
                        }
                    }

                    execute {
                        handler.serverStatus.collect {
                            state.serverStatus = it
                            logger.trace("Updated Server Status to {}", it)
                        }
                    }
                }
            }

        status = SourceStatusListener.Status.CONNECTED
    }

    private val currentApplicationInfo: ApplicationInfo
        get() {
            val packageInfo = try {
                service.packageManager.getPackageInfo(service.packageName, 0)
            } catch (ex: PackageManager.NameNotFoundException) {
                logger.error("Cannot find package info for pRMT app")
                null
            }

            @Suppress("DEPRECATION")
            return ApplicationInfo(
                manufacturer = Build.MANUFACTURER,
                model = Build.MODEL,
                osVersion = Build.VERSION.RELEASE,
                osVersionCode = Build.VERSION.SDK_INT,
                appVersion = packageInfo?.versionName,
                appVersionCode = packageInfo?.versionCode,
            )
        }

    // using versionCode
    private suspend fun processDeviceInfo() {
        deviceInfoCache.applyIfChanged(currentApplicationInfo) { deviceInfo ->
            applicationStatusExecutor.execute {
                send(
                    deviceInfoTopic,
                    ApplicationDeviceInfo(
                        currentTime,
                        deviceInfo.manufacturer,
                        deviceInfo.model,
                        OperatingSystem.ANDROID,
                        deviceInfo.osVersion,
                        deviceInfo.osVersionCode,
                        deviceInfo.appVersion,
                        deviceInfo.appVersionCode,
                    ),
                )
                withContext(Dispatchers.IO) {
                    prefs.edit().apply {
                        putString("manufacturer", deviceInfo.manufacturer)
                        putString("model", deviceInfo.model)
                        putString("operatingSystemVersion", deviceInfo.osVersion)
                        putInt("operatingSystemVersionCode", deviceInfo.osVersionCode ?: -1)
                        putString("appVersion", deviceInfo.appVersion)
                        putInt("appVersionCode", deviceInfo.appVersionCode ?: -1)
                    }.apply()
                }
            }
        }
    }

    private suspend fun processReferenceTime() {
        val ntpServer = ntpServer ?: return
        if (sntpClient.requestTime(ntpServer, 5000)) {
            val delay = sntpClient.roundTripTime / 1000.0
            val time = currentTime
            val ntpTime = (sntpClient.ntpTime + SystemClock.elapsedRealtime() - sntpClient.ntpTimeReference) / 1000.0

            send(
                ntpTopic,
                ApplicationExternalTime(
                    time,
                    ntpTime,
                    ntpServer,
                    ExternalTimeProtocol.SNTP,
                    delay,
                ),
            )
        }
    }

    private suspend fun processServerStatus() {
        val time = currentTime

        val status: ServerStatus = state.serverStatus.toServerStatus()
        val ipAddress = if (isProcessingIp) lookupIpAddress() else null
        logger.info("Server Status: {}; Device IP: {}", status, ipAddress)

        send(serverTopic, ApplicationServerStatus(time, status, ipAddress))
    }

    // Find Ip via NetworkInterfaces. Works via wifi, ethernet and mobile network
    // This finds both xx.xx.xx ip and rmnet. Last one is always ip.
    private fun lookupIpAddress(): String? {
        previousInetAddress = try {
            previousInetAddress?.takeUnless { NetworkInterface.getByInetAddress(it) == null }
                ?: NetworkInterface.getNetworkInterfaces().asSequence()
                    .flatMap { it.inetAddresses.asSequence() }
                    .findLast { !it.isLoopbackAddress && !it.isLinkLocalAddress }
        } catch (ex: SocketException) {
            logger.warn("No IP Address could be determined", ex)
            null
        }

        return previousInetAddress?.hostAddress
    }

    private suspend fun processUptime() {
        val uptime = (SystemClock.elapsedRealtime() - creationTimeStamp) / 1000.0
        send(uptimeTopic, ApplicationUptime(currentTime, uptime))
    }

    private suspend fun processRecordsSent() {
        val time = currentTime

        val recordsCached = state.cachedRecords.values
            .sumOf { if (it == NUMBER_UNKNOWN) 0L else it }

        val recordsSent = state.recordsSent

        logger.info("Number of records: {sent: {}, unsent: {}, cached: {}}",
            recordsSent, recordsCached, recordsCached)
        send(recordCountsTopic, ApplicationRecordCounts(time,
            recordsCached, recordsSent, recordsCached.toIntCapped()))
    }

    override fun onClose() {
        applicationStatusExecutor.stop {
            this.processor.stop()
        }
        cacheReceiver?.unregister()
        serverRecordsReceiver?.unregister()
        serverStatusReceiver?.unregister()
    }

    private suspend fun processTimeZone() {
        val now = System.currentTimeMillis()
        val tzOffset = TimeZone.getDefault().getOffset(now)
        tzOffsetCache.applyIfChanged(tzOffset / 1000) { offset ->
            applicationStatusExecutor.execute {
                send(
                    timeZoneTopic,
                    ApplicationTimeZone(
                        now / 1000.0,
                        offset,
                    ),
                )
                withContext(Dispatchers.IO) {
                    prefs.edit()
                        .putInt("timeZoneOffset", offset)
                        .apply()
                }
            }
        }
    }

    @Synchronized
    fun setTzUpdateRate(tzUpdateRate: Long, unit: TimeUnit) {
        if (tzUpdateRate > 0) { // enable timezone processor
            var processor = this.tzProcessor
            if (processor == null) {
                processor = OfflineProcessor(service) {
                    process = listOf { this@ApplicationStatusManager.processTimeZone() }
                    requestCode = APPLICATION_TZ_PROCESSOR_REQUEST_CODE
                    requestName = APPLICATION_TZ_PROCESSOR_REQUEST_NAME
                    interval(tzUpdateRate, unit)
                    wake = false
                }
                this.tzProcessor = processor

                if (this.state.status == SourceStatusListener.Status.CONNECTED) {
                    processor.start()
                }
            } else {
                processor.interval(tzUpdateRate, unit)
            }
        } else { // disable timezone processor
            this.tzProcessor?.let {
                applicationStatusExecutor.execute {
                    it.stop()
                }
                this.tzProcessor = null
            }
        }
    }

    fun setApplicationStatusUpdateRate(period: Long, unit: TimeUnit) {
        processor.interval(period, unit)
    }

    private data class ApplicationInfo(
        val manufacturer: String?,
        val model: String?,
        val osVersion: String?,
        val osVersionCode: Int?,
        val appVersion: String?,
        val appVersionCode: Int?,
    )

    companion object {
        private val logger = LoggerFactory.getLogger(ApplicationStatusManager::class.java)
        private const val NUMBER_UNKNOWN = -1L
        private const val APPLICATION_PROCESSOR_REQUEST_CODE = 72553575
        private const val APPLICATION_TZ_PROCESSOR_REQUEST_CODE = 72553576
        private const val APPLICATION_PROCESSOR_REQUEST_NAME = "org.radarbase.monitor.application.ApplicationStatusManager"
        private const val APPLICATION_TZ_PROCESSOR_REQUEST_NAME = "$APPLICATION_PROCESSOR_REQUEST_NAME.timeZone"

        private fun Long.toIntCapped(): Int = if (this <= Int.MAX_VALUE) toInt() else Int.MAX_VALUE

        private fun org.radarbase.android.kafka.ServerStatus?.toServerStatus(): ServerStatus = when (this) {
            org.radarbase.android.kafka.ServerStatus.CONNECTED,
            org.radarbase.android.kafka.ServerStatus.READY,
            org.radarbase.android.kafka.ServerStatus.UPLOADING -> ServerStatus.CONNECTED
            org.radarbase.android.kafka.ServerStatus.DISCONNECTED,
            org.radarbase.android.kafka.ServerStatus.DISABLED,
            org.radarbase.android.kafka.ServerStatus.UPLOADING_FAILED -> ServerStatus.DISCONNECTED
            else -> ServerStatus.UNKNOWN
        }
    }
}
