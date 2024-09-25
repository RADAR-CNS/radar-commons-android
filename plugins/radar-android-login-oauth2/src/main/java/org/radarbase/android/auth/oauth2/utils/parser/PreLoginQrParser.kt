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

package org.radarbase.android.auth.oauth2.utils.parser

import org.json.JSONException
import org.json.JSONObject
import org.radarbase.android.auth.AppAuthState
import org.radarbase.android.auth.AuthService.Companion.BASE_URL_PROPERTY
import org.radarbase.android.auth.AuthService.Companion.PRIVACY_POLICY_URL_PROPERTY
import org.radarbase.android.auth.AuthStringParser
import org.radarbase.android.auth.oauth2.OAuth2LoginManager.Companion.OAUTH2_SOURCE_TYPE
import java.io.IOException

class PreLoginQrParser(private val currentState: AppAuthState): AuthStringParser {

    @Throws(IOException::class)
    override fun parse(value: String): AppAuthState {
        try {
            val json = JSONObject(value)
            return currentState.alter {
                projectId = json.getString("projectId")
                attributes[BASE_URL_PROPERTY] = json.getString("baseUrl")
                attributes[PRIVACY_POLICY_URL_PROPERTY] = json.getString("privacyPolicyUrl")
                needsRegisteredSources = true
                authenticationSource = OAUTH2_SOURCE_TYPE
            }
        } catch (exception: JSONException) {
            throw IOException("Failed to parse json string $value", exception)
        }
    }
}