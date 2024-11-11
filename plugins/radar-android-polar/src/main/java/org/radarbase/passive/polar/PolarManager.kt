package org.radarbase.passive.polar

import PolarUtils
import android.annotation.SuppressLint
import android.content.Context.POWER_SERVICE
import android.os.PowerManager
import android.util.Log
import com.polar.sdk.api.PolarBleApi
import com.polar.sdk.api.PolarBleApiCallback
import com.polar.sdk.api.PolarBleApiDefaultImpl.defaultImplementation
import com.polar.sdk.api.errors.PolarInvalidArgument
import com.polar.sdk.api.model.*
import io.reactivex.rxjava3.android.schedulers.AndroidSchedulers
import io.reactivex.rxjava3.core.Flowable
import io.reactivex.rxjava3.disposables.Disposable
import org.radarbase.android.data.DataCache
import org.radarbase.android.source.AbstractSourceManager
import org.radarbase.android.source.SourceStatusListener
import org.radarbase.android.util.CoroutineTaskExecutor
import org.radarcns.kafka.ObservationKey
import org.radarcns.passive.polar.*
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.TimeUnit

class PolarManager(
    polarService: PolarService,
) : AbstractSourceManager<PolarService, PolarState>(polarService) {

    private val accelerationTopic: DataCache<ObservationKey, PolarAcceleration> =
        createCache("android_polar_acceleration", PolarAcceleration())
    private val batteryLevelTopic: DataCache<ObservationKey, PolarBatteryLevel> =
        createCache("android_polar_battery_level", PolarBatteryLevel())
    private val ecgTopic: DataCache<ObservationKey, PolarEcg> =
        createCache("android_polar_ecg", PolarEcg())
    private val heartRateTopic: DataCache<ObservationKey, PolarHeartRate> =
        createCache("android_polar_heart_rate", PolarHeartRate())
    private val ppIntervalTopic: DataCache<ObservationKey, PolarPpInterval> =
        createCache("android_polar_pulse_to_pulse_interval", PolarPpInterval())
    private val ppgTopic: DataCache<ObservationKey, PolarPpg> =
        createCache("android_polar_ppg", PolarPpg())

    private val polarExecutor = CoroutineTaskExecutor(this::class.simpleName!!)
    private var wakeLock: PowerManager.WakeLock? = null

    private lateinit var api: PolarBleApi

    private var deviceId: String? = null
    private var isDeviceConnected: Boolean = false

    private var autoConnectDisposable: Disposable? = null
    private var hrDisposable: Disposable? = null
    private var ecgDisposable: Disposable? = null
    private var accDisposable: Disposable? = null
    private var ppiDisposable: Disposable? = null
    private var ppgDisposable: Disposable? = null

    init {
        status = SourceStatusListener.Status.DISCONNECTED // red icon
        name = service.getString(R.string.polarDisplayName)
    }

    @SuppressLint("WakelockTimeout")
    override fun start(acceptableIds: Set<String>) {

        status = SourceStatusListener.Status.READY // blue loading

        connectToPolarSDK()

        register()
        polarExecutor.start()
        polarExecutor.execute {
            wakeLock = (service.getSystemService(POWER_SERVICE) as PowerManager?)?.let { pm ->
                pm.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "org.radarcns.polar:PolarManager")
                    .also { it.acquire() }
            }
        }

    }

    private fun connectToPolarSDK() {
        logger.debug("Connecting to Polar API")
        api = defaultImplementation(
            service.applicationContext,
            setOf(
                PolarBleApi.PolarBleSdkFeature.FEATURE_HR,
                PolarBleApi.PolarBleSdkFeature.FEATURE_POLAR_SDK_MODE,
                PolarBleApi.PolarBleSdkFeature.FEATURE_BATTERY_INFO,
                PolarBleApi.PolarBleSdkFeature.FEATURE_POLAR_H10_EXERCISE_RECORDING,
                PolarBleApi.PolarBleSdkFeature.FEATURE_POLAR_ONLINE_STREAMING,
                PolarBleApi.PolarBleSdkFeature.FEATURE_POLAR_DEVICE_TIME_SETUP,
                PolarBleApi.PolarBleSdkFeature.FEATURE_DEVICE_INFO
            )
        )
        api.setApiLogger { str: String -> Log.d("P-SDK", str) }
        api.setApiCallback(object : PolarBleApiCallback() {
            override fun blePowerStateChanged(powered: Boolean) {
                logger.debug("BluetoothStateChanged $powered")
                status = if (!powered) {
                    SourceStatusListener.Status.DISCONNECTED // red circle
                } else {
                    SourceStatusListener.Status.READY // blue loading
                }
            }

            override fun deviceConnected(polarDeviceInfo: PolarDeviceInfo) {
                logger.debug("Device connected ${polarDeviceInfo.deviceId}")
                service.savePolarDevice(polarDeviceInfo.deviceId)
                deviceId = polarDeviceInfo.deviceId
                name = polarDeviceInfo.name

                if (deviceId != null) {
                    isDeviceConnected = true
                    status = SourceStatusListener.Status.CONNECTED // green circle
                }
            }

            override fun deviceConnecting(polarDeviceInfo: PolarDeviceInfo) {
                status = SourceStatusListener.Status.CONNECTING // green dots
                logger.debug("Device connecting ${polarDeviceInfo.deviceId}")
            }

            override fun deviceDisconnected(polarDeviceInfo: PolarDeviceInfo) {
                logger.debug("Device disconnected ${polarDeviceInfo.deviceId}")
                isDeviceConnected = false
                disconnect()
            }

            override fun bleSdkFeatureReady(
                identifier: String,
                feature: PolarBleApi.PolarBleSdkFeature
            ) {

                if (isDeviceConnected) {
                    logger.debug("Feature ready {} for {}", feature, deviceId)

                    when (feature) {
                        PolarBleApi.PolarBleSdkFeature.FEATURE_POLAR_DEVICE_TIME_SETUP ->
                            setDeviceTime(deviceId)

                        PolarBleApi.PolarBleSdkFeature.FEATURE_POLAR_ONLINE_STREAMING -> {
                            streamHR()
                            streamEcg()
                            streamAcc()
                            streamPpi()
                            streamPpg()
                        }

                        else -> {
                            logger.debug("No feature was ready")
                        }
                    }
                } else {
                    logger.debug("No device was connected")
                }
            }

            override fun disInformationReceived(identifier: String, uuid: UUID, value: String) {
                logger.debug("Firmware: " + identifier + " " + value.trim { it <= ' ' })
            }

            override fun batteryLevelReceived(identifier: String, level: Int) {
                val batteryLevel = level.toFloat() / 100.0f
                state.batteryLevel = batteryLevel
                logger.debug("Battery level $level%, which is $batteryLevel at $currentTime")
                polarExecutor.execute {
                    send(
                        batteryLevelTopic,
                        PolarBatteryLevel(name, currentTime, currentTime, batteryLevel)
                    )
                }
            }
        })

        api.setApiLogger { s: String -> logger.debug("POLAR_API: {}", s) }

        try {
            deviceId = service.getPolarDevice()
            if (deviceId == null) {
                logger.debug("Searching for Polar devices")
                connectToPolarDevice()
            } else {
                logger.debug("Connecting to Polar device $deviceId")
                api.connectToDevice(deviceId!!)
            }
        } catch (a: PolarInvalidArgument) {
            a.printStackTrace()
        }
    }

    override fun onClose() {
        super.onClose()
        if (autoConnectDisposable != null && !autoConnectDisposable!!.isDisposed) {
            autoConnectDisposable?.dispose()
        }
        if (hrDisposable != null && !hrDisposable!!.isDisposed) {
            hrDisposable?.dispose()
        }
        if (ecgDisposable != null && !ecgDisposable!!.isDisposed) {
            ecgDisposable?.dispose()
        }
        if (accDisposable != null && !accDisposable!!.isDisposed) {
            accDisposable?.dispose()
        }
        if (ppiDisposable != null && !ppiDisposable!!.isDisposed) {
            ppiDisposable?.dispose()
        }
        if (ppgDisposable != null && !ppgDisposable!!.isDisposed) {
            ppgDisposable?.dispose()
        }

        disconnectToPolarSDK(deviceId)
        polarExecutor.stop {
            wakeLock?.release()
        }

    }

    private fun connectToPolarDevice() {
        autoConnectToPolarSDK()
    }

    private fun autoConnectToPolarSDK() {
        if (autoConnectDisposable != null && !autoConnectDisposable!!.isDisposed) {
            autoConnectDisposable?.dispose()
        }

        autoConnectDisposable = Flowable.interval(0, 5, TimeUnit.SECONDS)
            .subscribe({
                if (deviceId == null || !isDeviceConnected) {
                    searchPolarDevice(true)
                }
            }, { error: Throwable -> logger.error("Searching auto Polar devices failed: $error") })
    }

    private fun searchPolarDevice(force: Boolean = false): Disposable? {
        try {
            return api.searchForDevice()
                .subscribe({ device: PolarDeviceInfo ->
                    logger.debug("Device found: ${device.deviceId} ${device.name}")
                    if (deviceId == null || force) {
                        api.connectToDevice(device.deviceId)
                    }
                },
                    { error: Throwable ->
                        logger.error("Search for Polar devices failed: $error")
                    })
        } catch (a: PolarInvalidArgument) {
            a.printStackTrace()
        }
        return null
    }

    private fun disconnectToPolarSDK(deviceId: String?) {
        try {
            if (deviceId != null) {
                api.disconnectFromDevice(deviceId)
            }
            api.shutDown()
        } catch (e: Exception) {
            logger.error("Error occurred during shutdown: ${e.message}")
        }
    }

    fun setDeviceTime(deviceId: String?) {
        deviceId?.let { id ->
            val calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"))
            calendar.time = Date()
            api.setLocalTime(id, calendar)
                .observeOn(AndroidSchedulers.mainThread())
                .subscribe(
                    {
                        val timeSetString = "Time ${calendar.time} set to device"
                        logger.debug(timeSetString)
                    },
                    { error: Throwable -> logger.error("Set time failed: $error") }
                )
        } ?: run {
            logger.error("Device ID is null. Cannot set device time.")
        }
    }

    private fun getTimeNano(): Double {
        val nano = (System.currentTimeMillis() * 1_000_000L).toDouble()
        return nano / 1000_000_000L
    }

    fun streamHR() {
        logger.debug("start streamHR for $deviceId")
        val isDisposed = hrDisposable?.isDisposed ?: true
        if (isDisposed) {
            hrDisposable = deviceId?.let {
                api.startHrStreaming(it)
                    .doOnSubscribe { logger.debug("Subscribed to HrStreaming for $deviceId") }
                    .subscribe(
                        { hrData: PolarHrData ->
                            for (sample in hrData.samples) {
                                logger.debug(
                                    "HeartRate data for ${name}, ${deviceId}: HR ${sample.hr} " +
                                            "timeStamp: ${getTimeNano()} currentTime: $currentTime " +
                                            "R ${sample.rrsMs} rrAvailable: ${sample.rrAvailable} " +
                                            "contactStatus: ${sample.contactStatus} " +
                                            "contactStatusSupported: ${sample.contactStatusSupported}"
                                )
                                polarExecutor.execute {
                                    send(
                                        heartRateTopic,
                                        PolarHeartRate(
                                            name,
                                            getTimeNano(),
                                            currentTime,
                                            sample.hr,
                                            sample.rrsMs,
                                            sample.rrAvailable,
                                            sample.contactStatus,
                                            sample.contactStatusSupported
                                        )
                                    )
                                }

                            }
                        },
                        { error: Throwable ->
                            logger.error("HR stream failed for ${deviceId}. Reason $error")
                            hrDisposable = null
                        },
                        { logger.debug("HR stream for $deviceId complete") }
                    )
            }
        } else {
            hrDisposable?.dispose()
            logger.debug("HR stream disposed")
            hrDisposable = null
        }
    }

    fun streamEcg() {
        logger.debug("start streamECG for $deviceId")
        val isDisposed = ecgDisposable?.isDisposed ?: true
        if (isDisposed) {
            val settingMap = mapOf(
                PolarSensorSetting.SettingType.SAMPLE_RATE to 130,
                PolarSensorSetting.SettingType.RESOLUTION to 14
            )
            val ecgSettings = PolarSensorSetting(settingMap)
            deviceId?.let { deviceId ->
                ecgDisposable = api.startEcgStreaming(deviceId, ecgSettings)
                    .subscribe(
                        { polarEcgData: PolarEcgData ->
                            for (data in polarEcgData.samples) {
                                logger.debug(
                                    "ECG yV: ${data.voltage} timeStamp: ${
                                        PolarUtils.convertEpochPolarToUnixEpoch(data.timeStamp)
                                    } currentTime: $currentTime PolarTimeStamp: ${data.timeStamp}"
                                )
                                polarExecutor.execute {
                                    send(
                                        ecgTopic,
                                        PolarEcg(
                                            name,
                                            PolarUtils.convertEpochPolarToUnixEpoch(data.timeStamp),
                                            currentTime,
                                            data.voltage
                                        )
                                    )
                                }
                            }
                        },
                        { error: Throwable ->
                            logger.error("ECG stream failed. Reason $error")
                        },
                        { logger.debug("ECG stream complete") }
                    )
            }
        } else {
            ecgDisposable?.dispose()
        }
    }

    fun streamAcc() {
        val isDisposed = accDisposable?.isDisposed ?: true
        if (isDisposed) {
            val settingMap = mapOf(
                PolarSensorSetting.SettingType.SAMPLE_RATE to 25, // [50, 100, 200, 25]
                PolarSensorSetting.SettingType.RESOLUTION to 16, // [16]
                PolarSensorSetting.SettingType.RANGE to 2 // [2, 4, 8]
            )
            val accSettings = PolarSensorSetting(settingMap)
            deviceId?.let { deviceId ->
                accDisposable = api.startAccStreaming(deviceId, accSettings)
                    .subscribe(
                        { polarAccelerometerData: PolarAccelerometerData ->
                            for (data in polarAccelerometerData.samples) {
                                logger.debug(
                                    "ACC    x: ${data.x} y: ${data.y} z: ${data.z} timeStamp: ${
                                        PolarUtils.convertEpochPolarToUnixEpoch(data.timeStamp)
                                    } currentTime: $currentTime PolarTimeStamp: ${data.timeStamp}"
                                )
                                polarExecutor.execute {
                                    send(
                                        accelerationTopic,
                                        PolarAcceleration(
                                            name,
                                            PolarUtils.convertEpochPolarToUnixEpoch(data.timeStamp),
                                            currentTime,
                                            data.x,
                                            data.y,
                                            data.z
                                        )
                                    )
                                }
                            }
                        },
                        { error: Throwable ->
                            logger.error("ACC stream failed. Reason $error")
                        },
                        {
                            logger.debug("ACC stream complete")
                        }
                    )
            }
        } else {
            accDisposable?.dispose()
        }
    }

    fun streamPpg() {
        logger.debug("start streamPpg for $deviceId")
        val isDisposed = ppgDisposable?.isDisposed ?: true
        if (isDisposed) {
            val settingMap = mapOf(
                PolarSensorSetting.SettingType.SAMPLE_RATE to 55,
                PolarSensorSetting.SettingType.RESOLUTION to 22,
                PolarSensorSetting.SettingType.CHANNELS to 4
            )
            val ppgSettings = PolarSensorSetting(settingMap)
            deviceId?.let { deviceId ->
                ppgDisposable = api.startPpgStreaming(deviceId, ppgSettings)
                    .subscribe(
                        { polarPpgData: PolarPpgData ->
                            if (polarPpgData.type == PolarPpgData.PpgDataType.PPG3_AMBIENT1) {
                                for (data in polarPpgData.samples) {
                                    logger.debug(
                                        "PPG    ppg0: ${data.channelSamples[0]} ppg1: ${data.channelSamples[1]} ppg2: ${data.channelSamples[2]} ambient: ${data.channelSamples[3]} timeStamp: ${
                                            PolarUtils.convertEpochPolarToUnixEpoch(data.timeStamp)
                                        } currentTime: $currentTime PolarTimeStamp: ${data.timeStamp}"
                                    )
                                    polarExecutor.execute {
                                        send(
                                            ppgTopic,
                                            PolarPpg(
                                                name,
                                                PolarUtils.convertEpochPolarToUnixEpoch(data.timeStamp),
                                                currentTime,
                                                data.channelSamples[0],
                                                data.channelSamples[1],
                                                data.channelSamples[2],
                                                data.channelSamples[3]
                                            )
                                        )
                                    }
                                }
                            }
                        },
                        { error: Throwable ->
                            logger.error("ECG stream failed. Reason $error")
                        },
                        { logger.debug("ECG stream complete") }
                    )
            }
        } else {
            ecgDisposable?.dispose()
        }
    }

    fun streamPpi() {
        val isDisposed = ppiDisposable?.isDisposed ?: true
        if (isDisposed) {
            ppiDisposable = deviceId?.let {
                api.startPpiStreaming(it)
                    .subscribe(
                        { ppiData: PolarPpiData ->
                            for (sample in ppiData.samples) {
                                logger.debug(
                                    "PPI    ppi: ${sample.ppi} blocker: ${sample.blockerBit} " +
                                            "errorEstimate: ${sample.errorEstimate} " +
                                            "currentTime: $currentTime"
                                )
                                polarExecutor.execute {
                                    send(
                                        ppIntervalTopic,
                                        PolarPpInterval(
                                            name,
                                            currentTime,
                                            currentTime,
                                            sample.blockerBit,
                                            sample.errorEstimate,
                                            sample.hr,
                                            sample.ppi,
                                            sample.skinContactStatus,
                                            sample.skinContactSupported
                                        )
                                    )
                                }
                            }
                        },
                        { error: Throwable ->
                            logger.error("PPI stream failed. Reason $error")
                        },
                        { logger.debug("PPI stream complete") }
                    )
            }
        } else {
            ppiDisposable?.dispose()
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(PolarManager::class.java)
    }
}


