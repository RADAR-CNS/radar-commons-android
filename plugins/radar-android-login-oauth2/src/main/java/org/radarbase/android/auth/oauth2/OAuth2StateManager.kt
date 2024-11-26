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

package org.radarbase.android.auth.oauth2

import android.app.Activity
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import android.content.SharedPreferences
import android.net.Uri
import androidx.annotation.AnyThread
import net.openid.appauth.*
import org.json.JSONException
import org.radarbase.android.RadarConfiguration
import org.radarbase.android.auth.AppAuthState
import org.radarbase.android.auth.AuthService
import org.radarbase.android.auth.LoginManager
import org.radarbase.android.auth.oauth2.OAuth2LoginManager.Companion.LOGIN_REFRESH_TOKEN
import org.radarbase.android.auth.oauth2.utils.client.OAuthClient
import org.radarbase.android.config.SingleRadarConfiguration
import org.radarbase.android.util.toPendingIntentFlag
import org.slf4j.LoggerFactory

/**
 * Manages OAuth2 authentication states and login flow by utilizing AppAuth-Android library.
 * This class handles OAuth2 login, token refresh, and state persistence using
 * SharedPreferences. It interacts with the AuthorizationService to manage OAuth2
 * requests and responses It manages [AuthState] to be used in AppAuth-Android library and
 * [AppAuthState] for [AuthService] functioning.
 *
 * @param context the context in which the state manager operates.
 * @param client the OAuthClient instance used for OAuth2 communication.
 */
class OAuth2StateManager(context: Context, private var client: OAuthClient?) {
    private val mPrefs: SharedPreferences = context.getSharedPreferences(STORE_NAME, Context.MODE_PRIVATE)
    private var mCurrentAuthState: AuthState
    private val oAuthService: AuthorizationService? = null
    private var authorizedState: AppAuthState? = null

    init {
        mCurrentAuthState = readState()
    }

    /**
     * Initiates the OAuth 2.0 login process.
     * It constructs an authorization request and initiates the OAuth authorization flow.
     *
     * @param context The AuthService instance responsible for handling login success or failure.
     * @param activityClass The class of the Activity to be launched for handling the login.
     * @param config The OAuth configuration containing URLs and client details.
     */
    @AnyThread
    @Synchronized
    fun login(context: AuthService, activityClass: Class<out Activity>, config: SingleRadarConfiguration) {
        val authorizeUri = Uri.parse(config.getString(RadarConfiguration.OAUTH2_AUTHORIZE_URL))
        val tokenUri = Uri.parse(config.getString(RadarConfiguration.OAUTH2_TOKEN_URL))
        val redirectUri = Uri.parse(config.getString(RadarConfiguration.OAUTH2_REDIRECT_URL))
        val clientId = config.getString(RadarConfiguration.OAUTH2_CLIENT_ID)

        val authConfig = AuthorizationServiceConfiguration(authorizeUri, tokenUri, null)

        val authRequestBuilder = AuthorizationRequest.Builder(
            authConfig, // the authorization service configuration
            clientId, // the client ID, typically pre-registered and static
            ResponseTypeValues.CODE, // the response_type value: we want a code
            redirectUri,  // the redirect URI to which the auth response is sent
        )

        oAuthService?.performAuthorizationRequest(
            authRequestBuilder.build(),
            PendingIntent.getActivity(
                context,
                OAUTH_INTENT_HANDLER_REQUEST_CODE,
                Intent(context, activityClass),
                PendingIntent.FLAG_ONE_SHOT.toPendingIntentFlag(),
            ),
        )
    }

    fun updateClient(newClient: OAuthClient){
        client = newClient
    }

    @AnyThread
    @Synchronized
    fun updateAfterAuthorization(authService: AuthService, intent: Intent?): AppAuthState? {
        if (intent == null) {
            return null
        }

        val resp = AuthorizationResponse.fromIntent(intent)
        val ex = AuthorizationException.fromIntent(intent)

        if (resp != null || ex != null) {
            mCurrentAuthState.update(resp, ex)
            writeState(mCurrentAuthState)
        }

        if (resp != null) {
            // authorization succeeded
            oAuthService?.performTokenRequest(
                resp.createTokenExchangeRequest(),
                processTokenResponse(authService),
            )
        } else if (ex != null) {
            authService.loginFailed(null, ex)
            return null
        }
        return authorizedState
    }

    /**
     * Refreshes the OAuth 2.0 access token using the stored refresh token.
     *
     * @param context
     * @param refreshToken The refresh token to be used for the token refresh request.
     */
    @Synchronized
    fun refresh(context: AuthService, refreshToken: String?) {
        // refreshToken does not originate from the current auth state.
        if (refreshToken != null && refreshToken != mCurrentAuthState.refreshToken) {
            try {
                val json = mCurrentAuthState.jsonSerialize()
                json.put("refreshToken", refreshToken)
                mCurrentAuthState = AuthState.jsonDeserialize(json)
            } catch (e: JSONException) {
                logger.error("Failed to update refresh token")
            }
        }
        // authorization succeeded
        oAuthService?.performTokenRequest(
            mCurrentAuthState.createTokenRefreshRequest(),
            processTokenResponse(context),
        )
    }

    /**
     * Processes the token response and updates the authentication state after the token exchange.
     * If successful, the authorized state is updated with the new access and refresh tokens.
     *
     * @param context The AuthService instance to handle login success or failure.
     */
    private fun processTokenResponse(
        context: AuthService
    ) = AuthorizationService.TokenResponseCallback { resp, ex ->
        resp ?: return@TokenResponseCallback context.loginFailed(null, ex)
        authorizedState= updateAfterTokenResponse(resp, context, ex)
        authorizedState?.let {
            context.loginSucceeded(null, it)
        } ?: throw SubjectFetchFailedException("Failed to retrieve subjects from management portal")
    }

    /**
     * Updates the [AppAuthState] after a successful token response and prepares the authorized state.
     *
     * @param response The token response from the server.
     * @param service The AuthService instance responsible for handling login success callback.
     * @param ex The exception in case of a failure during token exchange.
     * @return The updated [AppAuthState].
     */
    @AnyThread
    @Synchronized
    fun updateAfterTokenResponse(
        response: TokenResponse?,
        service: AuthService,
        ex: AuthorizationException?
    ): AppAuthState? {
        mCurrentAuthState.update(response, ex)
        writeState(mCurrentAuthState)
        val authorizedAuthState = AppAuthState {
            token =
                checkNotNull(mCurrentAuthState.accessToken) { "Missing access token after successful login" }
                    .also { addHeader("Authorization", "Bearer $it") }
            tokenType = LoginManager.AUTH_TYPE_BEARER
            this.expiration = mCurrentAuthState.accessTokenExpirationTime ?: 0L
            attributes[LOGIN_REFRESH_TOKEN] =
                checkNotNull(mCurrentAuthState.refreshToken) { "Missing refresh token after successful login" }
        }
        service.updateState(authorizedAuthState)
        return client?.requestSubjectsFromMp(authorizedAuthState)
    }

    @AnyThread
    @Synchronized
    private fun readState(): AuthState {
        val currentState = mPrefs.getString(KEY_STATE, null) ?: return AuthState()

        return try {
            AuthState.jsonDeserialize(currentState)
        } catch (ex: JSONException) {
            logger.warn("Failed to deserialize stored auth state - discarding", ex)
            writeState(null)
            AuthState()
        }

    }

    @AnyThread
    @Synchronized
    private fun writeState(state: AuthState?) {
        mPrefs.edit().apply {
            if (state != null) {
                putString(KEY_STATE, state.jsonSerializeString())
            } else {
                remove(KEY_STATE)
            }
        }.apply()
    }

    fun release() {
        oAuthService?.dispose()
    }

    companion object {
        private const val OAUTH_INTENT_HANDLER_REQUEST_CODE = 8422341

        private val logger = LoggerFactory.getLogger(OAuth2StateManager::class.java)

        private const val STORE_NAME = "AuthState"
        private const val KEY_STATE = "state"

        class SubjectFetchFailedException(message: String): RuntimeException(message)
    }
}
