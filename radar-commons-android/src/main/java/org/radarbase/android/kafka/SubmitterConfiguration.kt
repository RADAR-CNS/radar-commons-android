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

package org.radarbase.android.kafka

import org.radarbase.android.RadarConfiguration
import org.radarbase.android.config.SingleRadarConfiguration

data class SubmitterConfiguration(
    var userId: String? = null,
    var amountLimit: Int = 1000,
    var sizeLimit: Long = 5000000L,
    var uploadRate: Long = 10L,
    var uploadRateMultiplier: Int = 1,
) {
    fun configure(config: SingleRadarConfiguration) {
        uploadRate = config.getLong(RadarConfiguration.KAFKA_UPLOAD_RATE_KEY, uploadRate)
        amountLimit = config.getInt(RadarConfiguration.KAFKA_RECORDS_SEND_LIMIT_KEY, amountLimit)
        sizeLimit = config.getLong(RadarConfiguration.KAFKA_RECORDS_SIZE_LIMIT_KEY, sizeLimit)
    }
}
