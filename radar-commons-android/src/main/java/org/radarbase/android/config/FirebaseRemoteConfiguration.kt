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

package org.radarbase.android.config

import android.content.Context
import android.os.Handler
import android.os.Looper
import androidx.annotation.XmlRes
import com.google.android.gms.tasks.OnFailureListener
import com.google.android.gms.tasks.OnSuccessListener
import com.google.firebase.analytics.FirebaseAnalytics
import com.google.firebase.remoteconfig.FirebaseRemoteConfig
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.withContext
import org.radarbase.android.RadarConfiguration
import org.radarbase.android.RadarConfiguration.Companion.PROJECT_ID_KEY
import org.radarbase.android.RadarConfiguration.Companion.USER_ID_KEY
import org.radarbase.android.auth.AppAuthState
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicBoolean

@Suppress("unused")
class FirebaseRemoteConfiguration(private val context: Context, inDevelopmentMode: Boolean, @XmlRes defaultSettings: Int) : RemoteConfig {
    private val firebase = FirebaseRemoteConfig.getInstance().apply {
        setDefaultsAsync(defaultSettings)
        isInDevelopmentMode = inDevelopmentMode
        fetch()
    }

    override var status = MutableStateFlow(RadarConfiguration.RemoteConfigStatus.INITIAL)

    private val onFailureListener: OnFailureListener = OnFailureListener { ex ->
        logger.info("Failed to fetch Firebase config", ex)
        status.value = RadarConfiguration.RemoteConfigStatus.ERROR
    }
    private val hasChange: AtomicBoolean = AtomicBoolean(false)

    override var lastFetch: Long = 0L
    override var cache: Map<String, String> = mapOf()
        private set

    private val handler: Handler = Handler(Looper.getMainLooper())
    private val onFetchCompleteHandler: OnSuccessListener<Void> = OnSuccessListener {
        // Once the config is successfully fetched it must be
        // activated before newly fetched values are returned.
        firebase.activate()
                .addOnSuccessListener {
                    cache = firebase.getKeysByPrefix("")
                            .mapNotNull { key ->
                                firebase.getValue(key).asString()
                                        .takeUnless { it.isEmpty() }
                                        ?.let { Pair(key, it) }
                            }
                            .toMap()

                    status.value = RadarConfiguration.RemoteConfigStatus.FETCHED
                }
                .addOnFailureListener(onFailureListener)
    }
    private var isInDevelopmentMode: Boolean = false
    private var firebaseKeys: Set<String> = HashSet(firebase.getKeysByPrefix(""))

    init {
        status.value = RadarConfiguration.RemoteConfigStatus.READY
    }

    /**
     * Fetch the configuration from the firebase server.
     * @param maxCacheAgeMillis seconds
     * @return fetch task or null status is [RadarConfiguration.RemoteConfigStatus.UNAVAILABLE].
     */
    override suspend fun doFetch(maxCacheAgeMillis: Long) {
        if (status.value == RadarConfiguration.RemoteConfigStatus.UNAVAILABLE) {
            return
        }
        val task = firebase.fetch(maxCacheAgeMillis / 1000L)
        synchronized(this) {
            status.value = RadarConfiguration.RemoteConfigStatus.FETCHING
            task.addOnSuccessListener(onFetchCompleteHandler)
            task.addOnFailureListener(onFailureListener)
        }
    }

    override suspend fun updateWithAuthState(appAuthState: AppAuthState?) {
        appAuthState ?: return
        val userId = appAuthState.userId ?: return
        val projectId = appAuthState.projectId ?: return
        val baseUrl = appAuthState.baseUrl ?: return
        withContext(Dispatchers.IO) {
            FirebaseAnalytics.getInstance(context).apply {
                setUserId(userId)
                setUserProperty(USER_ID_KEY, userId.limit(36))
                setUserProperty(PROJECT_ID_KEY, projectId.limit(36))
                setUserProperty(RadarConfiguration.BASE_URL_KEY, baseUrl.limit(36))
            }
        }
    }

    override suspend fun stop() {
        // do nothing
    }

    companion object {
        private val logger = LoggerFactory.getLogger(FirebaseRemoteConfiguration::class.java)
        private const val FIREBASE_FETCH_TIMEOUT_MS_DEFAULT = 12 * 60 * 60 * 1000L


        private fun String?.limit(numChars: Int): String? {
            return if (this != null && length > numChars) {
                substring(0, numChars)
            } else {
                this
            }
        }
    }
}
