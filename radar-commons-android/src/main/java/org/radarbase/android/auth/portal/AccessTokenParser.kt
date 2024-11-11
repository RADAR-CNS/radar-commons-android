package org.radarbase.android.auth.portal

import org.json.JSONException
import org.json.JSONObject
import org.radarbase.android.auth.AppAuthState
import org.radarbase.android.auth.AuthStringParser
import org.radarbase.android.auth.LoginManager.Companion.AUTH_TYPE_BEARER
import org.radarbase.android.auth.portal.ManagementPortalClient.Companion.MP_REFRESH_TOKEN_PROPERTY
import org.radarbase.android.auth.portal.ManagementPortalClient.Companion.SOURCE_IDS_PROPERTY
import org.radarbase.android.auth.portal.ManagementPortalLoginManager.Companion.SOURCE_TYPE
import org.radarbase.android.util.optNonEmptyString
import java.io.IOException
import java.util.concurrent.TimeUnit

class AccessTokenParser(private val state: AppAuthState.Builder) : AuthStringParser {

    @Throws(IOException::class)
    override suspend fun parse(value: JSONObject): AppAuthState.Builder {
        var refreshToken = state.attributes[MP_REFRESH_TOKEN_PROPERTY]
        try {
            val accessToken = value.getString("access_token")
            refreshToken = value.optNonEmptyString("refresh_token")
                ?: refreshToken?.takeIf { it.isNotEmpty() }
                        ?: throw IOException("Missing refresh token")
            return state.apply {
                attributes.apply {
                    put(MP_REFRESH_TOKEN_PROPERTY, refreshToken)
                    put(SOURCE_IDS_PROPERTY, value.optJSONArray("sources")?.join(",") ?: "")
                    setHeader("Authorization", "Bearer $accessToken")
                    token = accessToken
                    tokenType = AUTH_TYPE_BEARER
                    userId = value.getString("sub")
                    expiration = TimeUnit.SECONDS.toMillis(
                        value.optLong(
                            "expires_in",
                            3600L
                        )
                    ) + System.currentTimeMillis()
                    needsRegisteredSources = true
                    authenticationSource = SOURCE_TYPE
                }
            }
        } catch (ex: JSONException) {
            throw IOException("Failed to parse json string $value", ex)
        }
    }
}
