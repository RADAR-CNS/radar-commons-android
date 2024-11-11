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

import android.app.Application
import android.os.Bundle
import androidx.annotation.CallSuper
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.radarbase.android.config.CombinedRadarConfig
import org.radarbase.android.config.LocalConfiguration
import org.radarbase.android.config.RemoteConfig
import org.radarbase.android.source.SourceService
import org.radarbase.android.util.NotificationHandler
import org.slf4j.impl.HandroidLoggerAdapter

/** Provides the name and some metadata of the main activity  */
abstract class AbstractRadarApplication : Application(), RadarApplication {

    override lateinit var notificationHandler: NotificationHandler

    override lateinit var configuration: RadarConfiguration

    override fun configureProvider(bundle: Bundle) {}
    override fun onSourceServiceInvocation(service: SourceService<*>, bundle: Bundle, isNew: Boolean) {}
    override fun onSourceServiceDestroy(service: SourceService<*>) {}

    @CallSuper
    override fun onCreate() {
        super.onCreate()
        setupLogging()

        runBlocking(Dispatchers.Default) {
            launch {
                notificationHandler = NotificationHandler(this@AbstractRadarApplication)
                notificationHandler.onCreate()
            }
            launch {
                configuration = createConfiguration()
            }
        }
    }

    protected open fun setupLogging() {
        // initialize crashlytics
        HandroidLoggerAdapter.DEBUG = BuildConfig.DEBUG
        HandroidLoggerAdapter.APP_NAME = packageName
        HandroidLoggerAdapter.enableLoggingToFirebaseCrashlytics()
    }

    /**
     * Create a RadarConfiguration object. At implementation, the Firebase version needs to be set
     * for this as well as the defaults.
     *
     * @return configured RadarConfiguration
     */
    protected open suspend fun createConfiguration(): RadarConfiguration = coroutineScope {
        val localConfigurationJob = async(Dispatchers.IO) { LocalConfiguration(this@AbstractRadarApplication) }
        val remoteConfigJob = async { createRemoteConfiguration() }
        val defaultConfigJob = async { createDefaultConfiguration() }
        CombinedRadarConfig(
            localConfigurationJob.await(),
            remoteConfigJob.await(),
            defaultConfigJob.await(),
        )
    }

    /**
     * Create remote configuration objects. For example,
     * [org.radarbase.android.config.FirebaseRemoteConfiguration].
     *
     * @return configured RadarConfiguration
     */
    protected open suspend fun createRemoteConfiguration(): List<RemoteConfig> = listOf()

    /**
     * Create default configuration for the app.
     */
    protected open suspend fun createDefaultConfiguration(): Map<String, String> = mapOf()
}
