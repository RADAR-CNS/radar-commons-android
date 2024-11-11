package org.radarbase.android.config

import kotlinx.coroutines.flow.Flow
import org.radarbase.android.RadarConfiguration
import org.radarbase.android.auth.AppAuthState
import org.slf4j.LoggerFactory

interface RemoteConfig {
    val status: Flow<RadarConfiguration.RemoteConfigStatus>
    var lastFetch: Long
    val cache: Map<String, String>

    suspend fun doFetch(maxCacheAgeMillis: Long)

    suspend fun fetch(maxCacheAge: Long) {
        if (lastFetch + maxCacheAge < System.currentTimeMillis()) {
            doFetch(maxCacheAge)
        } else {
            logger.info("No fetch needed. Old values still in cache.")
        }
    }

    suspend fun forceFetch() {
        lastFetch = 0
        doFetch(0)
    }

    suspend fun updateWithConfig(config: SingleRadarConfiguration?) = Unit
    suspend fun updateWithAuthState(appAuthState: AppAuthState?)

    companion object {
        private val logger = LoggerFactory.getLogger(RemoteConfig::class.java)
    }

    suspend fun stop()
}
