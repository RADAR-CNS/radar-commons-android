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

import android.Manifest.permission.ACCESS_BACKGROUND_LOCATION
import android.Manifest.permission.ACCESS_COARSE_LOCATION
import android.Manifest.permission.ACCESS_FINE_LOCATION
import android.annotation.SuppressLint
import android.content.pm.PackageManager
import android.os.Build
import androidx.core.content.ContextCompat
import com.google.android.gms.common.api.ApiException
import com.google.android.libraries.places.api.Places
import com.google.android.libraries.places.api.model.AddressComponent
import com.google.android.libraries.places.api.model.Place
import com.google.android.libraries.places.api.model.PlaceLikelihood
import com.google.android.libraries.places.api.net.FetchPlaceRequest
import com.google.android.libraries.places.api.net.FetchPlaceResponse
import com.google.android.libraries.places.api.net.FindCurrentPlaceRequest
import com.google.android.libraries.places.api.net.FindCurrentPlaceResponse
import com.google.android.libraries.places.api.net.PlacesClient
import org.radarbase.android.data.DataCache
import org.radarbase.android.source.AbstractSourceManager
import org.radarbase.android.source.SourceStatusListener
import org.radarbase.android.util.BroadcastRegistration
import org.radarbase.android.util.CoroutineTaskExecutor
import org.radarbase.android.util.OfflineProcessor
import org.radarbase.android.util.register
import org.radarbase.passive.google.places.GooglePlacesService.Companion.GOOGLE_PLACES_API_KEY_DEFAULT
import org.radarcns.kafka.ObservationKey
import org.radarcns.passive.google.GooglePlacesInfo
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.math.pow

class GooglePlacesManager(service: GooglePlacesService, @get: Synchronized private var apiKey: String, private val placeExecutor: CoroutineTaskExecutor) : AbstractSourceManager<GooglePlacesService, GooglePlacesState>(service) {
    private val placesInfoTopic: DataCache<ObservationKey, GooglePlacesInfo> = createCache(
        "android_google_places_info",
        GooglePlacesInfo()
    )

    private val placesProcessor: OfflineProcessor
    // Delay in seconds for exponential backoff
    private val maxDelay: Long = 43200
    private val baseDelay: Long = 300

    private val isPermissionGranted: Boolean
        get() = checkLocationPermissions()
    private val placesClientCreated: Boolean
        get() = service.placesClientCreated.get()
    private val placesClient: PlacesClient?
        get() = service.placesClient
    private val fromBroadcastRegistration: Boolean
        get()= state.fromBroadcast.get()
    var shouldFetchPlaceId: Boolean
        get() = state.shouldFetchPlaceId.get()
        set(value) = state.shouldFetchPlaceId.set(value)
    var shouldFetchAdditionalInfo: Boolean
        get() = state.shouldFetchAdditionalInfo.get()
        set(value) = state.shouldFetchAdditionalInfo.set(value)
    var limitByPlacesCount: Int
        get() = state.limitByPlacesCount
        set(value) { state.limitByPlacesCount = value }
    var limitByPlacesLikelihood: Double
        get() = state.limitByPlacesLikelihood
        set(value) { state.limitByPlacesLikelihood = value }
    var additionalFetchDelay: Long
        get() = state.additionalFetchDelay
        set(value) { state.additionalFetchDelay = value }
    private val currentPlaceFields: List<Place.Field>
        get() = if (shouldFetchPlaceId||shouldFetchAdditionalInfo) listOf(Place.Field.TYPES, Place.Field.ID) else listOf(Place.Field.TYPES)
    private val detailsPlaceFields: List<Place.Field> =  listOf(Place.Field.ADDRESS_COMPONENTS)

    private var placesBroadcastReceiver: BroadcastRegistration? = null
    private var isRecentlySent: AtomicBoolean = AtomicBoolean(false)

    private lateinit var currentPlaceRequest:FindCurrentPlaceRequest
    private lateinit var detailsPlaceRequest:FetchPlaceRequest

    init {
        name = service.getString(R.string.googlePlacesDisplayName)
        placesProcessor = OfflineProcessor(service) {
            process = listOf(this@GooglePlacesManager::processPlacesData)
            requestCode = GOOGLE_PLACES_REQUEST_CODE
            requestName = GOOGLE_PLACES_REQUEST_NAME
            wake = false
        }
    }

    override fun start(acceptableIds: Set<String>) {
        if (service.internalError.get()) {
            status = SourceStatusListener.Status.UNAVAILABLE
            return
        }
        register()
        status = SourceStatusListener.Status.READY
        placesProcessor.start()
        placeExecutor.execute { if (!placesClientCreated) {
            service.createPlacesClient()
        }
        if (!placesClientCreated && !Places.isInitialized()) {
            updateApiKey(apiKey)
        } }
        updateConnected()
        placesBroadcastReceiver = service.broadcaster?.register(DEVICE_LOCATION_CHANGED) { _, _ ->
            if (placesClientCreated) {
                placeExecutor.execute {
                    state.fromBroadcast.set(true)
                    processPlacesData()
                    state.fromBroadcast.set(false)
                }
            }
        }
    }

    fun updateApiKey(apiKey: String) {
        when (apiKey) {
            GOOGLE_PLACES_API_KEY_DEFAULT -> {
                logger.error("API-key is empty, disconnecting now")
                service.internalError.set(true)
                disconnect()
            }
            else -> {
                synchronized(this)  {
                this@GooglePlacesManager.apiKey = apiKey
                }
                if (!Places.isInitialized()) {
                    initializePlacesClient()
                    if (!placesClientCreated) {
                        service.createPlacesClient()
                        updateConnected()
                    }
                }
            }
        }
    }

    private fun updateConnected() {
        if (Places.isInitialized()) {
            status = SourceStatusListener.Status.CONNECTED
        }
    }

    private fun initializePlacesClient() {
        try {
            Places.initialize(service.applicationContext, apiKey)
            updateConnected()
        } catch (ex: Exception) {
            logger.error("Exception while initializing places API", ex)
            service.internalError.set(true)
            disconnect()
        }
    }

    @SuppressLint("MissingPermission")
    private suspend fun processPlacesData() {
        placeExecutor.execute {
            if (isPermissionGranted) {
                val client = placesClient ?: return@execute
                currentPlaceRequest = FindCurrentPlaceRequest.newInstance(currentPlaceFields)
                   // resetting the backoff time in SharedPreferences once the plugin works successfully
                if (service.numOfAttempts > 0) {
                        service.preferences.edit()
                            .putInt(NUMBER_OF_ATTEMPTS_KEY, 0).apply()
                    }
                    client.findCurrentPlace(currentPlaceRequest)
                        .addOnSuccessListener { response: FindCurrentPlaceResponse ->
                            val limitLikelihood = limitByPlacesLikelihood
                            val countLikelihood = limitByPlacesCount
                            val placeLikelihoods = response.placeLikelihoods.toMutableList()
                            if (limitLikelihood >= 0.0)  {
                                placeLikelihoods.removeAll { it.likelihood < limitLikelihood }
                            }
                            placeLikelihoods.sortByDescending { it.likelihood }
                            doSend(
                                if (countLikelihood >= 0) {
                                    placeLikelihoods.take(countLikelihood)
                                } else {
                                    placeLikelihoods
                                }
                            )
                        }
                        .addOnFailureListener { ex ->
                            when (ex.javaClass) {
                                ApiException::class.java -> {
                                    val exception = ex as ApiException
                                    handleApiException(exception, exception.statusCode)
                                }
                                else -> logger.error("Exception while fetching current place", ex)
                            }
                        }
                } else {
                    logger.warn("Location permissions not granted yet")
                }
            }
        }

    private fun doSend(places: List<PlaceLikelihood>) {
        places.forEach { likelihood ->
            val types = likelihood.place.placeTypes
            val placeId: String? = if (shouldFetchPlaceId) likelihood.place.id else null
            val placesInfoBuilder = GooglePlacesInfo.newBuilder().apply {
                time = currentTime
                timeReceived = currentTime
                if (types != null) {
                    placeType1 = types.getOrNull(0)
                    placeType2 = types.getOrNull(1)
                    placeType3 = types.getOrNull(2)
                    placeType4 = types.getOrNull(3)
                }
                this.likelihood = likelihood.likelihood
                fromBroadcast = fromBroadcastRegistration
                city = null
                state = null
                country = null
                this.placeId = placeId
            }
            if (shouldFetchAdditionalInfo && !isRecentlySent.get()) {
                val id: String? = likelihood.place.id
                id?.let {
                    detailsPlaceRequest = FetchPlaceRequest.newInstance(it, detailsPlaceFields)

                    val client = checkNotNull(placesClient) { "Google Places client unexpectedly became null" }
                    client.fetchPlace(detailsPlaceRequest)
                            .addOnSuccessListener { placeInfo: FetchPlaceResponse ->
                                val addressComponents = placeInfo.place.addressComponents?.asList()
                                val city = findComponent(addressComponents, CITY_KEY)
                                val state = findComponent(addressComponents, STATE_KEY)
                                val country = findComponent(addressComponents, COUNTRY_KEY)
                                placeExecutor.execute {
                                    send(placesInfoTopic, placesInfoBuilder.apply {
                                        this.city = city
                                        this.state = state
                                        this.country = country
                                        this.placeId = it
                                    }.build())
                                    updateRecentlySent()
                                    logger.info("Google Places data with additional info sent")
                                }
                            }
                            .addOnFailureListener { ex ->
                                when (ex.javaClass) {
                                    ApiException::class.java -> {
                                        val exception = ex as ApiException
                                        handleApiException(exception, exception.statusCode)
                                    }
                                    else -> {
                                        placeExecutor.execute {
                                            send(placesInfoTopic,
                                                placesInfoBuilder.apply { this.placeId = it }
                                                    .build()
                                            )
                                            logger.info("Google Places data sent")
                                        }
                                    }
                                }
                            }
                }
            } else {
                placeExecutor.execute {
                    send(placesInfoTopic, placesInfoBuilder.build())
                }
            }
        }
    }

    /**
     * The [FindCurrentPlaceRequest] fetches a list of places. When [shouldFetchAdditionalInfo] is true, it makes extra [FetchPlaceRequest] calls for each place in CurrentPlaceResponse. To reduce network overhead, additional data is fetched only if at least additionalFetchDelay (ms) have passed since the last request.
     */
    private fun updateRecentlySent() {
        isRecentlySent.set(true)
        placeExecutor.delay(additionalFetchDelay) {
            isRecentlySent.set(false)
        }
    }

    private fun findComponent(addressComponents: MutableList<AddressComponent>?, component: String): String? =
             addressComponents?.firstOrNull { address ->
                 address.types.any { it.equals(component) }
             }?.name

    private fun handleApiException(exception: ApiException, statusCode: Int) {
        when (statusCode) {
            INVALID_API_CODE -> {
                logger.error("Invalid Api key, disconnecting now")
                service.internalError.set(true)
                disconnect()
            }
            LOCATION_UNAVAILABLE_CODE -> logger.warn("Location not enabled yet")
            else -> logger.error("ApiException occurred with status code $statusCode", exception)
        }
    }

    fun placesFetchInterval(interval: Long, intervalUnit: TimeUnit) {
        placesProcessor.interval(interval, intervalUnit)
    }

    fun reScan() {
        status = SourceStatusListener.Status.DISCONNECTED
    }

    private fun checkLocationPermissions(): Boolean {
        val hasPermissions = if (Build.VERSION.SDK_INT < Build.VERSION_CODES.Q) {
            ((ContextCompat.checkSelfPermission(service, ACCESS_COARSE_LOCATION) == PackageManager.PERMISSION_GRANTED) || ContextCompat.checkSelfPermission(service, ACCESS_FINE_LOCATION) == PackageManager.PERMISSION_GRANTED) }
        else {
            ((ContextCompat.checkSelfPermission(service, ACCESS_COARSE_LOCATION) == PackageManager.PERMISSION_GRANTED) || ContextCompat.checkSelfPermission(service, ACCESS_FINE_LOCATION) == PackageManager.PERMISSION_GRANTED) &&
                    ContextCompat.checkSelfPermission(service, ACCESS_BACKGROUND_LOCATION) == PackageManager.PERMISSION_GRANTED
        }
        check(hasPermissions) { "Location permission is missing for Google Places" }
        return hasPermissions
    }

    override fun onClose() {
            val currentDelay = (baseDelay + 2.0.pow(service.numOfAttempts)).toLong()
            placeExecutor.execute {
                if (currentDelay < maxDelay) {
                    service.numOfAttempts++
                    service.preferences
                        .edit()
                        .putInt(NUMBER_OF_ATTEMPTS_KEY, service.numOfAttempts).apply()
                }
                try {
                    placesBroadcastReceiver?.unregister()
                } catch (ex: IllegalStateException) {
                    logger.warn("Places receiver already unregistered in broadcast")
                }
                placesBroadcastReceiver = null
// Not using Places.deinitialize for now as it may prevent PlacesClient from being created during a plugin reload. Additionally, multiple calls to Places.initialize do not impact memory or CPU.
//                Places.deinitialize()
                placesProcessor.stop()
            }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(GooglePlacesManager::class.java)

        private const val GOOGLE_PLACES_REQUEST_CODE = 483579
        private const val GOOGLE_PLACES_REQUEST_NAME = "org.radarbase.passive.google.places.GooglePlacesManager.PLACES_PROCESS_REQUEST"
        private const val INVALID_API_CODE = 9011
        private const val LOCATION_UNAVAILABLE_CODE = 8
        private const val CITY_KEY = "locality"
        private const val STATE_KEY = "administrative_area_level_1"
        private const val COUNTRY_KEY = "country"
        const val NUMBER_OF_ATTEMPTS_KEY = "number_of_attempts_key"
        const val DEVICE_LOCATION_CHANGED = "org.radarbase.passive.google.places.GooglePlacesManager.DEVICE_LOCATION_CHANGED"
    }
}