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

package org.radarbase.android

import android.annotation.SuppressLint
import android.content.Context
import kotlinx.coroutines.flow.StateFlow
import org.radarbase.android.auth.AppAuthState
import org.radarbase.android.config.SingleRadarConfiguration
import java.util.*

interface RadarConfiguration {
    val status: StateFlow<RemoteConfigStatus>
    val latestConfig: SingleRadarConfiguration
    val config: StateFlow<SingleRadarConfiguration>

    enum class RemoteConfigStatus {
        UNAVAILABLE, INITIAL, ERROR, READY, FETCHING, FETCHED, PARTIALLY_FETCHED
    }

    /**
     * Adds a new or updated setting to the local configuration. This will be persisted to
     * SharedPreferences. Using this will override remote settings. Setting it to `null`
     * means that the default value in code will be used, not the Firebase setting. Use
     * [.reset] to completely unset any local configuration.
     *
     * @param key configuration name
     * @param value configuration value
     * @return previous local value for given name, if any
     */
    fun put(key: String, value: Any): String?

    suspend fun persistChanges()

    /**
     * Reset configuration to remote config values. If no keys are given, all local
     * settings are reset, otherwise only the given keys are reset.
     * @param keys configuration names
     */
    suspend fun reset(vararg keys: String)

    /**
     * Fetch the remote configuration from server if it is outdated.
     */
    suspend fun fetch()

    /**
     * Force fetching the remote configuration from server, even if it is not outdated.
     */
    suspend fun forceFetch()

    /**
     * Adds base URL from auth state to configuration.
     */
    suspend fun updateWithAuthState(context: Context, appAuthState: AppAuthState?)

    companion object {
        const val RADAR_CONFIGURATION_CHANGED = "org.radarcns.android.RadarConfiguration.CHANGED"
        const val KAFKA_REST_PROXY_URL_KEY = "kafka_rest_proxy_url"
        const val SCHEMA_REGISTRY_URL_KEY = "schema_registry_url"
        const val MANAGEMENT_PORTAL_URL_KEY = "management_portal_url"
        const val PROJECT_ID_KEY = "radar_project_id"
        const val USER_ID_KEY = "radar_user_id"
        const val READABLE_USER_ID_KEY = "readable_user_id"
        const val EXTERNAL_USER_ID_KEY = "external_user_id"
        const val BASE_URL_KEY = "radar_base_url"
        const val SOURCE_ID_KEY = "source_id"
        const val SEND_OVER_DATA_HIGH_PRIORITY = "send_over_data_high_priority_only"
        const val TOPICS_HIGH_PRIORITY = "topics_high_priority"
        const val UI_REFRESH_RATE_KEY = "ui_refresh_rate_millis"
        const val KAFKA_UPLOAD_RATE_KEY = "kafka_upload_rate"
        const val DATABASE_COMMIT_RATE_KEY = "database_commit_rate"
        const val KAFKA_RECORDS_SEND_LIMIT_KEY = "kafka_records_send_limit"
        const val KAFKA_RECORDS_SIZE_LIMIT_KEY = "kafka_records_size_limit"
        const val SENDER_CONNECTION_TIMEOUT_KEY = "sender_connection_timeout"
        const val FIREBASE_FETCH_TIMEOUT_MS_KEY = "firebase_fetch_timeout_ms"
        const val FETCH_TIMEOUT_MS_KEY = "fetch_timeout_ms"
        const val FETCH_TIMEOUT_MS_DEFAULT = 14_400_000L
        const val START_AT_BOOT = "start_at_boot"
        const val DEVICE_SERVICES_TO_CONNECT = "device_services_to_connect"
        const val PLUGINS = "plugins"
        const val KAFKA_UPLOAD_MINIMUM_BATTERY_LEVEL = "kafka_upload_minimum_battery_level"
        const val KAFKA_UPLOAD_REDUCED_BATTERY_LEVEL = "kafka_upload_reduced_battery_level"
        const val MAX_CACHE_SIZE = "cache_max_size_bytes"
        const val SEND_ONLY_WITH_WIFI = "send_only_with_wifi"
        const val SEND_BINARY_CONTENT = "send_binary_content"
        const val SEND_WITH_COMPRESSION = "send_with_compression"
        const val UNSAFE_KAFKA_CONNECTION = "unsafe_kafka_connection"
        const val OAUTH2_AUTHORIZE_URL = "oauth2_authorize_url"
        const val OAUTH2_TOKEN_URL = "oauth2_token_url"
        const val OAUTH2_REDIRECT_URL = "oauth2_redirect_url"
        const val OAUTH2_CLIENT_ID = "oauth2_client_id"
        const val OAUTH2_CLIENT_SECRET = "oauth2_client_secret"
        const val ENABLE_BLUETOOTH_REQUESTS = "enable_bluetooth_requests"

        const val SEND_ONLY_WITH_WIFI_DEFAULT = true
        const val SEND_OVER_DATA_HIGH_PRIORITY_DEFAULT = true
        const val SEND_BINARY_CONTENT_DEFAULT = true

        @SuppressLint("ApplySharedPref")
        fun getOrSetUUID(context: Context, key: String): String {
            val prefs = context.getSharedPreferences("global", Context.MODE_PRIVATE)
            return synchronized(RadarConfiguration::class.java) {
                prefs.getString(key, null)
                        ?:  UUID.randomUUID().toString()
                                .also { prefs.edit()
                                        .putString(key, it)
                                        .commit() // commit immediately to avoid races
                                }
            }
        }
    }
}
