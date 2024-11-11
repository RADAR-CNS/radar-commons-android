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

import android.content.Context
import android.content.SharedPreferences
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import org.radarbase.android.config.SingleRadarConfiguration
import org.radarbase.android.source.BaseSourceState
import org.radarbase.android.source.SourceManager
import org.radarbase.android.source.SourceService
import org.radarbase.android.util.StageLevels

class PhoneLocationService : SourceService<BaseSourceState>() {
    override val defaultState: BaseSourceState
        get() = BaseSourceState()

    override val isBluetoothConnectionRequired: Boolean = false

    private val prefLock = Mutex()
    private val preferences: SharedPreferences
        get() = getSharedPreferences("org.radarbase.passive.phone.PhoneLocationService", Context.MODE_PRIVATE)

    suspend fun withPreferences(block: SharedPreferences.() -> Unit) = prefLock.withLock {
        preferences.block()
    }
    suspend fun withMutablePreferences(block: SharedPreferences.(SharedPreferences.Editor) -> Boolean) = prefLock.withLock {
        val prefs = preferences
        val editor = prefs.edit()
        if (prefs.block(editor)) {
            withContext(Dispatchers.IO) {
                editor.commit()
            }
        }
    }

    override fun createSourceManager() = PhoneLocationManager(this)

    override fun configureSourceManager(manager: SourceManager<BaseSourceState>, config: SingleRadarConfiguration) {
        manager as PhoneLocationManager
        manager.setBatteryLevels(StageLevels(
                minimum = config.getFloat(PHONE_LOCATION_BATTERY_LEVEL_MINIMUM, MINIMUM_BATTERY_LEVEL_DEFAULT),
                reduced = config.getFloat(PHONE_LOCATION_BATTERY_LEVEL_REDUCED, REDUCED_BATTERY_LEVEL_DEFAULT)))
        manager.setIntervals(PhoneLocationManager.LocationPollingIntervals(
                gps = config.getLong(PHONE_LOCATION_GPS_INTERVAL, LOCATION_GPS_INTERVAL_DEFAULT),
                gpsReduced = config.getLong(PHONE_LOCATION_GPS_INTERVAL_REDUCED, LOCATION_GPS_INTERVAL_REDUCED_DEFAULT),
                network = config.getLong(PHONE_LOCATION_NETWORK_INTERVAL, LOCATION_NETWORK_INTERVAL_DEFAULT),
                networkReduced = config.getLong(PHONE_LOCATION_NETWORK_INTERVAL_REDUCED, LOCATION_NETWORK_INTERVAL_REDUCED_DEFAULT)))
        manager.isAbsoluteLocation = !config.getBoolean(PHONE_LOCATION_RELATIVE, true)

    }

    companion object {
        private const val PHONE_LOCATION_GPS_INTERVAL = "phone_location_gps_interval"
        private const val PHONE_LOCATION_GPS_INTERVAL_REDUCED = "phone_location_gps_interval_reduced"
        private const val PHONE_LOCATION_NETWORK_INTERVAL = "phone_location_network_interval"
        private const val PHONE_LOCATION_NETWORK_INTERVAL_REDUCED = "phone_location_network_interval_reduced"
        private const val PHONE_LOCATION_BATTERY_LEVEL_REDUCED = "phone_location_battery_level_reduced"
        private const val PHONE_LOCATION_BATTERY_LEVEL_MINIMUM = "phone_location_battery_level_minimum"
        private const val PHONE_LOCATION_RELATIVE = "phone_location_relative"

        internal const val LOCATION_GPS_INTERVAL_DEFAULT = 15 * 60L // seconds
        internal const val LOCATION_GPS_INTERVAL_REDUCED_DEFAULT = 4 * LOCATION_GPS_INTERVAL_DEFAULT // seconds
        internal const val LOCATION_NETWORK_INTERVAL_DEFAULT = 5 * 60L // seconds
        internal const val LOCATION_NETWORK_INTERVAL_REDUCED_DEFAULT = 4 * LOCATION_NETWORK_INTERVAL_DEFAULT // seconds

        private const val MINIMUM_BATTERY_LEVEL_DEFAULT = 0.15f
        private const val REDUCED_BATTERY_LEVEL_DEFAULT = 0.3f
    }
}
