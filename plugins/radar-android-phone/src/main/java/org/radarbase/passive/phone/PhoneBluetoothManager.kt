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

import android.Manifest
import android.annotation.SuppressLint
import android.bluetooth.BluetoothAdapter
import android.bluetooth.BluetoothDevice
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.content.SharedPreferences
import android.content.pm.PackageManager
import android.os.Build
import androidx.core.app.ActivityCompat
import kotlinx.coroutines.SupervisorJob
import org.radarbase.android.data.DataCache
import org.radarbase.android.source.AbstractSourceManager
import org.radarbase.android.source.BaseSourceState
import org.radarbase.android.source.SourceStatusListener
import org.radarbase.android.util.BluetoothStateReceiver.Companion.bluetoothAdapter
import org.radarbase.android.util.BluetoothStateReceiver.Companion.hasBluetoothPermission
import org.radarbase.android.util.CoroutineTaskExecutor
import org.radarbase.android.util.HashGenerator
import org.radarbase.android.util.OfflineProcessor
import org.radarcns.kafka.ObservationKey
import org.radarcns.passive.phone.PairedState
import org.radarcns.passive.phone.PhoneBluetoothDeviceScanned
import org.radarcns.passive.phone.PhoneBluetoothDevices
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit

class PhoneBluetoothManager(service: PhoneBluetoothService) : AbstractSourceManager<PhoneBluetoothService, BaseSourceState>(service) {
    private val processor: OfflineProcessor
    private val bluetoothDevicesTopic: DataCache<ObservationKey, PhoneBluetoothDevices> = createCache(
        "android_phone_bluetooth_devices",
        PhoneBluetoothDevices()
    )
    private val bluetoothScannedTopic: DataCache<ObservationKey, PhoneBluetoothDeviceScanned> = createCache(
        "android_phone_bluetooth_device_scanned",
        PhoneBluetoothDeviceScanned()
    )

    private val bluetoothTaskExecutor = CoroutineTaskExecutor(this::class.simpleName!!)

    private var bluetoothBroadcastReceiver: BroadcastReceiver? = null
    private val hashGenerator: HashGenerator = HashGenerator(service, "bluetooth_devices")
    private val preferences: SharedPreferences
        get() = service.getSharedPreferences(PhoneBluetoothManager::class.java.name, Context.MODE_PRIVATE)

    private var hashSaltReference: Int = 0

    init {
        name = service.getString(R.string.bluetooth_devices)
        processor = OfflineProcessor(service) {
            process = listOf(this@PhoneBluetoothManager::processBluetoothDevices)
            requestCode = SCAN_DEVICES_REQUEST_CODE
            requestName = ACTION_SCAN_DEVICES
            wake = true
        }
        preferences.apply {
             if (contains(HASH_SALT_REFERENCE)) {
                 hashSaltReference = getInt(HASH_SALT_REFERENCE, -1)
             } else {
                 val random = ThreadLocalRandom.current()
                 while (hashSaltReference == 0) {
                     hashSaltReference = random.nextInt()
                 }
                 edit().putInt(HASH_SALT_REFERENCE, hashSaltReference).apply()
             }
        }
    }

    override fun start(acceptableIds: Set<String>) {
        status = SourceStatusListener.Status.READY
        register()
        processor.start()
        bluetoothTaskExecutor.start(SupervisorJob())
        status = SourceStatusListener.Status.CONNECTED
    }

    private suspend fun processBluetoothDevices() {
        val bluetoothAdapter = service.bluetoothAdapter
        if (bluetoothAdapter == null) {
            logger.error("Bluetooth is not available.")
            return
        }
        if (!service.hasBluetoothPermission) {
            logger.error("Cannot initiate Bluetooth scan without scan permissions")
            return
        }
        if (bluetoothAdapter.isEnabled) {
            val filter = IntentFilter().apply {
                addAction(BluetoothDevice.ACTION_FOUND)
                addAction(BluetoothAdapter.ACTION_DISCOVERY_FINISHED)
            }

            bluetoothBroadcastReceiver = object : BroadcastReceiver() {
                private var numberOfDevices: Int = 0

                val hasConnectPermission = Build.VERSION.SDK_INT < Build.VERSION_CODES.S
                        || ActivityCompat.checkSelfPermission(service, Manifest.permission.BLUETOOTH_CONNECT) == PackageManager.PERMISSION_GRANTED

                @SuppressLint("MissingPermission")
                override fun onReceive(context: Context, intent: Intent) {
                    val action = intent.action ?: return
                    when (action) {
                        BluetoothDevice.ACTION_FOUND -> {
                            numberOfDevices++

                            val device: BluetoothDevice = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                                intent.getParcelableExtra(BluetoothDevice.EXTRA_DEVICE, BluetoothDevice::class.java)
                            } else {
                                @Suppress("DEPRECATION")
                                intent.getParcelableExtra(BluetoothDevice.EXTRA_DEVICE)
                            } ?: return

                            val macAddress = device.address
                            val macAddressHash: ByteBuffer = hashGenerator.createHashByteBuffer(macAddress + "$hashSaltReference")

                            val scannedTopicBuilder = PhoneBluetoothDeviceScanned.newBuilder().apply {
                                time = currentTime
                                timeReceived = currentTime
                            }

                            val pairedDevices: Set<BluetoothDevice> = if (hasConnectPermission) bluetoothAdapter.bondedDevices else emptySet()

                            pairedDevices.forEach { bd ->
                                val mac = bd.address
                                val hash =
                                    hashGenerator.createHashByteBuffer(mac + "$hashSaltReference")

                                bluetoothTaskExecutor.execute {
                                    send(bluetoothScannedTopic, scannedTopicBuilder.apply {
                                        this.macAddressHash = hash
                                        this.pairedState = bd.bondState.toPairedState()
                                        this.hashSaltReference = hashSaltReference
                                    }.build())
                                }
                            }
                            bluetoothTaskExecutor.execute {
                                send(bluetoothScannedTopic, scannedTopicBuilder.apply {
                                    this.macAddressHash = macAddressHash
                                    this.pairedState = device.bondState.toPairedState()
                                    this.hashSaltReference = hashSaltReference
                                }.build())
                            }
                        }

                        BluetoothAdapter.ACTION_DISCOVERY_FINISHED -> {
                            service.unregisterReceiver(this)
                            bluetoothBroadcastReceiver = null

                            val bondedDevices = if (hasConnectPermission) bluetoothAdapter.bondedDevices.size else -1

                            if (!isClosed) {
                                val time = currentTime
                                bluetoothTaskExecutor.execute {
                                    send(
                                        bluetoothDevicesTopic, PhoneBluetoothDevices(
                                            time, time, bondedDevices, numberOfDevices, true
                                        )
                                    )
                                }
                            }
                        }
                    }
                }
            }

            service.registerReceiver(bluetoothBroadcastReceiver, filter)
            bluetoothAdapter.startDiscovery()
        } else {
            val time = currentTime
            send(bluetoothDevicesTopic, PhoneBluetoothDevices(
                    time, time, null, null, false))
        }
    }

    override fun onClose() {
        bluetoothTaskExecutor.stop {
            processor.stop()
        }
        bluetoothBroadcastReceiver?.let {
            try {
                service.unregisterReceiver(it)
            } catch (ex: IllegalArgumentException) {
                logger.warn("Bluetooth receiver already unregistered in broadcast")
            }
            bluetoothBroadcastReceiver = null
        }
    }

    internal fun setCheckInterval(checkInterval: Long, intervalUnit: TimeUnit) {
        processor.interval(checkInterval, intervalUnit)
    }

    companion object {
        private val logger = LoggerFactory.getLogger(PhoneBluetoothManager::class.java)

        private const val SCAN_DEVICES_REQUEST_CODE = 3248902
        private const val ACTION_SCAN_DEVICES = "org.radarbase.passive.phone.PhoneBluetoothManager.ACTION_SCAN_DEVICES"
        private const val HASH_SALT_REFERENCE = "hash_salt_reference"

        private fun Int.toPairedState(): PairedState = when(this) {
            10 -> PairedState.NOT_PAIRED
            11 -> PairedState.PAIRING
            12 -> PairedState.PAIRED
            else -> PairedState.UNKNOWN
        }
    }
}
