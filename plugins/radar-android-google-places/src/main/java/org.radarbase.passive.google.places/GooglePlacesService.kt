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

package org.radarbase.passive.google.places

import android.content.Context
import android.content.SharedPreferences
import androidx.localbroadcastmanager.content.LocalBroadcastManager
import com.google.android.libraries.places.api.Places
import com.google.android.libraries.places.api.net.PlacesClient
import org.radarbase.android.config.SingleRadarConfiguration
import org.radarbase.android.source.SourceManager
import org.radarbase.android.source.SourceService
import org.radarbase.android.source.SourceStatusListener
import org.radarbase.android.util.ChangeRunner
import org.radarbase.android.util.CoroutineTaskExecutor
import org.radarbase.android.util.send
import org.radarbase.passive.google.places.GooglePlacesManager.Companion.NUMBER_OF_ATTEMPTS_KEY
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.math.pow

class GooglePlacesService: SourceService<GooglePlacesState>() {
    private val apiKey: ChangeRunner<String> = ChangeRunner()
    lateinit var preferences: SharedPreferences
    val placesClientCreated = AtomicBoolean(false)
    var broadcaster: LocalBroadcastManager? = null
        private set
    private lateinit var placeHandler: CoroutineTaskExecutor
    var placesClient: PlacesClient? = null
        @Synchronized get() = if (placesClientCreated.get()) field else null

    val internalError = AtomicBoolean(false)
    private val baseDelay: Long = 300
    @get: Synchronized
    @set: Synchronized
    var numOfAttempts: Int = -1

    override fun onCreate() {
        super.onCreate()
        placeHandler = CoroutineTaskExecutor(this::class.simpleName!!).apply {
            start()
        }
        preferences = getSharedPreferences(GooglePlacesService::class.java.name, Context.MODE_PRIVATE)
        broadcaster = LocalBroadcastManager.getInstance(this)
    }

    override fun configureSourceManager(manager: SourceManager<GooglePlacesState>, config: SingleRadarConfiguration) {
        manager as GooglePlacesManager

        apiKey.applyIfChanged(config.getString(GOOGLE_PLACES_API_KEY, GOOGLE_PLACES_API_KEY_DEFAULT)) {
           placeHandler.execute {
                   try {
                       Places.initialize(this.applicationContext, it)
                       manager.updateApiKey(it)
                   } catch (ex: IllegalArgumentException) {
                       logger.error("API Key can't be empty")
                   }
               }
           }

        manager.placesFetchInterval(config.getLong(GOOGLE_PLACES_FETCH_INTERVAL, GOOGLE_PLACES_FETCH_INTERVAL_DEFAULT), TimeUnit.SECONDS)
        manager.shouldFetchPlaceId = config.getBoolean(FETCH_GOOGLE_PLACES_ID_KEY, false)
        manager.shouldFetchAdditionalInfo = config.getBoolean(FETCH_GOOGLE_PLACES_ADDITIONAL_INFO, false)
        manager.limitByPlacesCount = config.getInt(FETCH_GOOGLE_PLACE_COUNT_BOUND, FETCH_GOOGLE_PLACE_COUNT_NUMBER_DEFAULT)
        manager.limitByPlacesLikelihood = config.getFloat(FETCH_GOOGLE_PLACE_LIKELIHOOD_BOUND, GOOGLE_FETCH_PLACE_LIKELIHOOD_BOUND_DEFAULT.toFloat()).toDouble()
        manager.additionalFetchDelay = config.getLong(ADDITIONAL_FETCH_NETWORK_DELAY, ADDITIONAL_FETCH_NETWORK_DELAY_DEFAULT)
    }

    override fun sourceStatusUpdated(
        manager: SourceManager<*>,
        status: SourceStatusListener.Status
    ) {
        if (status == SourceStatusListener.Status.DISCONNECTED && internalError.get()) {
            numOfAttempts = preferences.getInt(NUMBER_OF_ATTEMPTS_KEY, 0)
            val currentDelay = (baseDelay + 2.0.pow(numOfAttempts)).toLong()
            logger.info("Disconnecting {} now, will reconnect after {} seconds", manager.name, currentDelay)
            placeHandler.delay(currentDelay * 1000) {
                internalError.set(false)
                val currentManager = sourceManager
                (currentManager as? GooglePlacesManager)?.reScan()
                if (currentManager == null) {
                    super.status.value = SourceStatusListener.Status.DISCONNECTED
                }
                stopRecording()
            }
        } else {
            super.sourceStatusUpdated(manager, status)
        }
    }

    @Synchronized
    fun createPlacesClient() {
        try {
            placesClient = Places.createClient(this)
            placesClientCreated.set(true)
        } catch (ex: IllegalStateException) {
            placesClientCreated.set(false)
            logger.error("Places client has not been initialized yet.")
        }
    }


    override val defaultState: GooglePlacesState
        get() = GooglePlacesState()

    override fun createSourceManager(): GooglePlacesManager = GooglePlacesManager(this, GOOGLE_PLACES_API_KEY_DEFAULT, placeHandler)

    override val isBluetoothConnectionRequired: Boolean = false

    override fun onDestroy() {
        super.onDestroy()
        placeHandler.stop()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(GooglePlacesService::class.java)

        const val GOOGLE_PLACES_API_KEY = "places_api_key"
        const val GOOGLE_PLACES_FETCH_INTERVAL = "places_interval_seconds"
        const val FETCH_GOOGLE_PLACES_ID_KEY = "should_fetch_places_id"
        const val FETCH_GOOGLE_PLACES_ADDITIONAL_INFO = "should_fetch_additional_places_info"
        const val FETCH_GOOGLE_PLACE_LIKELIHOOD_BOUND = "fetch_place_likelihoods_bound"
        const val FETCH_GOOGLE_PLACE_COUNT_BOUND = "fetch_place_likelihoods_count"
        const val ADDITIONAL_FETCH_NETWORK_DELAY = "additional_fetch_network_delay"

        const val GOOGLE_PLACES_FETCH_INTERVAL_DEFAULT = 600L
        const val GOOGLE_PLACES_API_KEY_DEFAULT = ""
        const val GOOGLE_FETCH_PLACE_LIKELIHOOD_BOUND_DEFAULT = -1.0
        const val FETCH_GOOGLE_PLACE_COUNT_NUMBER_DEFAULT = -1
        const val ADDITIONAL_FETCH_NETWORK_DELAY_DEFAULT = 0L
    }
}