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
import android.location.Location
import android.location.LocationListener
import android.location.LocationManager
import android.os.Bundle
import androidx.localbroadcastmanager.content.LocalBroadcastManager
import org.radarbase.android.data.DataCache
import org.radarbase.android.source.AbstractSourceManager
import org.radarbase.android.source.BaseSourceState
import org.radarbase.android.source.SourceStatusListener
import org.radarbase.android.util.BatteryStageReceiver
import org.radarbase.android.util.ChangeRunner
import org.radarbase.android.util.CoroutineTaskExecutor
import org.radarbase.android.util.StageLevels
import org.radarbase.android.util.send
import org.radarbase.passive.google.places.GooglePlacesManager.Companion.DEVICE_LOCATION_CHANGED
import org.radarbase.passive.phone.PhoneLocationService.Companion.LOCATION_GPS_INTERVAL_DEFAULT
import org.radarbase.passive.phone.PhoneLocationService.Companion.LOCATION_GPS_INTERVAL_REDUCED_DEFAULT
import org.radarbase.passive.phone.PhoneLocationService.Companion.LOCATION_NETWORK_INTERVAL_DEFAULT
import org.radarbase.passive.phone.PhoneLocationService.Companion.LOCATION_NETWORK_INTERVAL_REDUCED_DEFAULT
import org.radarcns.kafka.ObservationKey
import org.radarcns.passive.phone.LocationProvider
import org.radarcns.passive.phone.PhoneRelativeLocation
import org.slf4j.LoggerFactory
import java.math.BigDecimal
import java.util.concurrent.ThreadLocalRandom

class PhoneLocationManager(context: PhoneLocationService) : AbstractSourceManager<PhoneLocationService, BaseSourceState>(context), LocationListener {
    private val locationTopic: DataCache<ObservationKey, PhoneRelativeLocation> = createCache(
        "android_phone_relative_location",
        PhoneRelativeLocation()
    )
    private val locationManager = service.getSystemService(Context.LOCATION_SERVICE) as LocationManager?
    private val locationExecutor = CoroutineTaskExecutor(this::class.simpleName!!)
    private val batteryLevelReceiver = BatteryStageReceiver(context, StageLevels(0.1f, 0.3f), ::onBatteryLevelChanged)
    private var latitudeReference: BigDecimal? = null
    private var longitudeReference: BigDecimal? = null
    private var altitudeReference: Double = 0.toDouble()
    private val frequency = ChangeRunner<BatteryStageReceiver.BatteryStage>()
    private val intervals = ChangeRunner(LocationPollingIntervals())
    private var isStarted: Boolean = false
    private var referenceId: Int = 0
    private lateinit var broadcaster: LocalBroadcastManager
    @Volatile
    var isAbsoluteLocation: Boolean = false

    init {
        name = service.getString(R.string.phoneLocationServiceDisplayName)
    }

    override fun start(acceptableIds: Set<String>) {
        locationManager ?: return
        register()
        locationExecutor.start()

        locationExecutor.execute {
            service.withMutablePreferences { editor ->
                latitudeReference = getString(LATITUDE_REFERENCE, null)
                    ?.let { BigDecimal(it) }

                longitudeReference = getString(LONGITUDE_REFERENCE, null)
                    ?.let { BigDecimal(it) }

                var editorWasUsed = false

                if (contains(ALTITUDE_REFERENCE)) {
                    try {
                        altitudeReference = Double.fromBits(getLong(ALTITUDE_REFERENCE, 0))
                    } catch (ex: ClassCastException) {
                        // to fix bug where this was stored as String
                        altitudeReference = getString(ALTITUDE_REFERENCE, "0.0")?.toDouble() ?: 0.0
                        editor.putLong(ALTITUDE_REFERENCE, altitudeReference.toRawBits())
                        editorWasUsed = true
                    }
                } else {
                    altitudeReference = Double.NaN
                }

                if (contains(REFERENCE_ID)) {
                    referenceId = getInt(REFERENCE_ID, -1)
                } else {
                    val random = ThreadLocalRandom.current()
                    while (referenceId == 0) {
                        referenceId = random.nextInt()
                    }
                    editor.putInt(REFERENCE_ID, referenceId)
                    editorWasUsed = true
                }

                editorWasUsed
            }
        }

        status = SourceStatusListener.Status.READY
        broadcaster = LocalBroadcastManager.getInstance(service)

        locationExecutor.execute {
            isStarted = true
            batteryLevelReceiver.register()
            status = SourceStatusListener.Status.CONNECTED
        }
    }

    override fun onLocationChanged(location: Location) {
        val eventTimestamp = location.time / 1000.0
        val timestamp = currentTime

        val provider = when(location.provider) {
            LocationManager.GPS_PROVIDER -> LocationProvider.GPS
            LocationManager.NETWORK_PROVIDER -> LocationProvider.NETWORK
            else -> LocationProvider.OTHER
        }

        val isAbsolute = isAbsoluteLocation

        // Coordinates in degrees from the first coordinate registered
        val latitude = offsetLatitude(location.latitude, isAbsolute)
        val longitude = offsetLongitude(location.longitude, isAbsolute)
        val altitude = if (location.hasAltitude()) offsetAltitude(location.altitude, isAbsolute) else null
        val accuracy = if (location.hasAccuracy()) location.accuracy else null
        val speed = if (location.hasSpeed()) location.speed else null
        val bearing = if (location.hasBearing()) location.bearing else null
        val reference = if (isAbsolute) 0 else referenceId

        val value = PhoneRelativeLocation(
                eventTimestamp, timestamp, reference, provider,
                latitude.normalize(), longitude.normalize(),
                altitude?.normalize(), accuracy?.normalize(), speed?.normalize(), bearing?.normalize())
        locationExecutor.execute {
            send(locationTopic, value)
        }

        broadcaster.send(DEVICE_LOCATION_CHANGED)
        logger.info("Location: {} {} {} {} {} {} {} {} {}", provider, eventTimestamp, latitude,
                longitude, accuracy, altitude, speed, bearing, timestamp)
    }

    @Deprecated("This callback will never be invoked on Android Q and above.")
    override fun onStatusChanged(provider: String, status: Int, extras: Bundle) {}

    override fun onProviderEnabled(provider: String) {}

    override fun onProviderDisabled(provider: String) {}

    @SuppressLint("MissingPermission")
    fun setLocationUpdateRate(periodGPS: Long, periodNetwork: Long) {
        locationExecutor.executeReentrant {
            if (!isStarted || locationManager == null) {
                return@executeReentrant
            }

            // Remove updates, if any
            locationManager.removeUpdates(this@PhoneLocationManager)

            // Initialize with last known and start listening
            when {
                periodGPS <= 0 -> logger.info("Location GPS gathering disabled in settings")
                locationManager.isProviderEnabled(LocationManager.GPS_PROVIDER) -> {
                    locationManager.getLastKnownLocation(LocationManager.GPS_PROVIDER)
                        ?.let { onLocationChanged(it) }
                    locationManager.requestLocationUpdates(LocationManager.GPS_PROVIDER, periodGPS * 1000, 0f, this@PhoneLocationManager)
                    logger.info("Location GPS listener activated and set to a period of {}", periodGPS)
                }
                else -> logger.warn("Location GPS listener not found")
            }

            when {
                periodNetwork <= 0 -> logger.info("Location network gathering disabled in settings")
                locationManager.isProviderEnabled(LocationManager.NETWORK_PROVIDER) -> {
                    locationManager.getLastKnownLocation(LocationManager.NETWORK_PROVIDER)
                        ?.let { onLocationChanged(it) }
                    locationManager.requestLocationUpdates(LocationManager.NETWORK_PROVIDER, periodNetwork * 1000, 0f, this@PhoneLocationManager)
                    logger.info("Location Network listener activated and set to a period of {}", periodNetwork)
                }
                else -> logger.warn("Location Network listener not found")
            }
        }
    }

    private fun offsetLatitude(absoluteLatitude: Double, isAbsolute: Boolean): Double {
        if (absoluteLatitude.isNaN()) {
            return Double.NaN
        } else if (isAbsolute) {
            return absoluteLatitude
        }

        val latitude = BigDecimal.valueOf(absoluteLatitude)
        if (latitudeReference == null) {
            // Create reference within 8 degrees of actual latitude
            // corresponds loosely to the UTM zones used to make flat coordinates estimations.
            val reference = ThreadLocalRandom.current().nextDouble(-4.0, 4.0) // interval [-4,4)
            latitudeReference = BigDecimal.valueOf(reference)
                    .also {
                        locationExecutor.execute {
                            service.withMutablePreferences { editor ->
                                editor.putString(LATITUDE_REFERENCE, it.toString())
                                true
                            }
                        }
                    }
        }

        return latitude.subtract(latitudeReference).toDouble()
    }

    private fun offsetLongitude(absoluteLongitude: Double, isAbsolute: Boolean): Double {
        if (absoluteLongitude.isNaN()) {
            return Double.NaN
        } else if (isAbsolute) {
            return absoluteLongitude
        }

        val longitude = BigDecimal.valueOf(absoluteLongitude)
        if (longitudeReference == null) {
            longitudeReference = longitude
            locationExecutor.execute {
                service.withMutablePreferences { editor ->
                    editor.putString(LONGITUDE_REFERENCE, longitude.toString())
                    true
                }
            }
        }

        val relativeLongitude = longitude.subtract(longitudeReference).toDouble()

        // Wraparound if relative longitude outside range of valid values [-180,180]
        // assumption: relative longitude in interval [-540,540]
        if (relativeLongitude > 180.0) {
            return relativeLongitude - 360.0
        } else if (relativeLongitude < -180.0) {
            return relativeLongitude + 360.0
        }

        return relativeLongitude
    }

    private fun offsetAltitude(absoluteAltitude: Double, isAbsolute: Boolean): Float {
        if (absoluteAltitude.isNaN()) {
            return Float.NaN
        } else if (isAbsolute) {
            return absoluteAltitude.toFloat()
        }

        if (altitudeReference.isNaN()) {
            altitudeReference = absoluteAltitude

            locationExecutor.execute {
                service.withMutablePreferences { editor ->
                    editor.putLong(ALTITUDE_REFERENCE, absoluteAltitude.toRawBits())
                    true
                }
            }
        }
        return (absoluteAltitude - altitudeReference).toFloat()
    }

    private fun onBatteryLevelChanged(stage: BatteryStageReceiver.BatteryStage) {
        locationExecutor.executeReentrant {
            frequency.applyIfChanged(stage) { resetPollingIntervals() }
        }
    }

    private fun resetPollingIntervals() {
        when (frequency.value) {
            BatteryStageReceiver.BatteryStage.FULL ->
                setLocationUpdateRate(intervals.value.gps, intervals.value.gpsReduced)
            BatteryStageReceiver.BatteryStage.REDUCED ->
                setLocationUpdateRate(intervals.value.network, intervals.value.networkReduced)
            BatteryStageReceiver.BatteryStage.EMPTY ->
                setLocationUpdateRate(0L, 0L)
        }
    }

    override fun onClose() {
        locationManager?.let { manager ->
            locationExecutor.stop {
                batteryLevelReceiver.unregister()
                manager.removeUpdates(this@PhoneLocationManager)
            }
        }
    }

    fun setBatteryLevels(stageLevels: StageLevels) {
        locationExecutor.execute {
            batteryLevelReceiver.stageLevels = stageLevels
        }
    }

    fun setIntervals(value: LocationPollingIntervals) {
        locationExecutor.execute {
            intervals.applyIfChanged(value) { resetPollingIntervals() }
        }
    }

    data class LocationPollingIntervals(
            val gps: Long = LOCATION_GPS_INTERVAL_DEFAULT,
            val gpsReduced: Long = LOCATION_GPS_INTERVAL_REDUCED_DEFAULT,
            val network: Long = LOCATION_NETWORK_INTERVAL_DEFAULT,
            val networkReduced: Long = LOCATION_NETWORK_INTERVAL_REDUCED_DEFAULT)

    companion object {
        private val logger = LoggerFactory.getLogger(PhoneLocationManager::class.java)

        // storage with keys
        private const val LATITUDE_REFERENCE = "latitude.reference"
        private const val REFERENCE_ID = "reference.id"
        private const val LONGITUDE_REFERENCE = "longitude.reference"
        private const val ALTITUDE_REFERENCE = "altitude.reference"

        /** Replace special float values with regular numbers.  */
        private fun Double.normalize(): Double? {
            if (isNaN()) {
                return null
            }
            return when(this) {
                Double.Companion.NEGATIVE_INFINITY -> -1e308
                Double.Companion.POSITIVE_INFINITY -> 1e308
                else -> this
            }
        }

        /** Replace special float values with regular numbers.  */
        private fun Float.normalize(): Float? {
            if (isNaN()) {
                return null
            }
            return when(this) {
                Float.Companion.NEGATIVE_INFINITY -> -3e38f
                Float.Companion.POSITIVE_INFINITY -> 3e38f
                else -> this
            }
        }
    }
}
