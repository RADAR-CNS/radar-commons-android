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