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

package org.radarbase.android.source

import android.content.ComponentName
import android.content.Context
import android.os.IBinder
import androidx.lifecycle.lifecycleScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import org.radarbase.android.RadarService
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class SourceServiceConnection<S : BaseSourceState>(
    private val radarService: RadarService,
    serviceClassName: String,
) : BaseServiceConnection<S>(serviceClassName) {
    val context: Context
        get() = radarService

    private var serviceJob: Job? = null
    private var sourceFailedJob: Job? = null

    override fun onServiceConnected(className: ComponentName?, service: IBinder?) {
        radarService.run {
            serviceJob = lifecycleScope.launch {
                sourceStatus
                    ?.collect {
                        logger.info("Source status changed of service: $serviceClassName")
                        radarService.sourceStatusUpdated(this@SourceServiceConnection, it)
                    }
            }
            sourceFailedJob = lifecycleScope.launch {
                sourceConnectFailed?.collect {
                    radarService.sourceFailedToConnect(it.sourceName, it.serviceClass)
                }
            }
        }

        if (!hasService()) {
            super.onServiceConnected(className, service)
            if (hasService()) {
                radarService.serviceConnected(this)
            }
        }
    }

    override fun onServiceDisconnected(className: ComponentName?) {
        serviceJob?.cancel()
        sourceFailedJob?.cancel()
        val hadService = hasService()
        super.onServiceDisconnected(className)

        if (hadService) {
            radarService.serviceDisconnected(this)
        }
    }
    companion object {
        private val logger: Logger = LoggerFactory.getLogger(SourceServiceConnection::class.java)
    }
}
