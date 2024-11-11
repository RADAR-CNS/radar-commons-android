package org.radarbase.android.auth.portal

import org.json.JSONException
import org.json.JSONObject
import org.radarbase.android.auth.AppAuthState
import org.radarbase.android.auth.AuthService.Companion.BASE_URL_PROPERTY
import org.radarbase.android.auth.AuthService.Companion.PRIVACY_POLICY_URL_PROPERTY
import org.radarbase.android.auth.AuthStringParser
import org.radarbase.android.auth.portal.ManagementPortalClient.Companion.MP_REFRESH_TOKEN_PROPERTY
import org.radarbase.android.auth.portal.ManagementPortalLoginManager.Companion.SOURCE_TYPE
import java.io.IOException

/**
 * Reads refreshToken and meta-data of token from json string and sets it as property in
 * [AppAuthState].
 */
class MetaTokenParser(private val currentState: AppAuthState.Builder) : AuthStringParser {

    @Throws(IOException::class)
    override suspend fun parse(value: JSONObject): AppAuthState.Builder {
        try {
            return currentState.apply {
                attributes.apply {
                    put(MP_REFRESH_TOKEN_PROPERTY, value.getString("refreshToken"))
                    put(PRIVACY_POLICY_URL_PROPERTY, value.getString("privacyPolicyUrl"))
                    put(BASE_URL_PROPERTY, value.getString("baseUrl"))
                    needsRegisteredSources = true
                    authenticationSource = SOURCE_TYPE
                }
            }
        } catch (ex: JSONException) {
            throw IOException("Failed to parse json string $value", ex)
        }
    }
}
