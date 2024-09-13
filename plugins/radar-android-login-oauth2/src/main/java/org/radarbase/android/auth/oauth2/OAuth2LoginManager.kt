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
import androidx.lifecycle.Observer
import org.json.JSONException
import org.json.JSONObject
import org.radarbase.android.RadarApplication.Companion.radarApp
import org.radarbase.android.RadarApplication.Companion.radarConfig
import org.radarbase.android.RadarConfiguration
import org.radarbase.android.RadarConfiguration.Companion.MANAGEMENT_PORTAL_URL_KEY
import org.radarbase.android.RadarConfiguration.Companion.UNSAFE_KAFKA_CONNECTION
import org.radarbase.android.auth.*
import org.radarbase.android.auth.AuthService.Companion.BASE_URL_PROPERTY
import org.radarbase.android.auth.oauth2.utils.client.OAuthClient
import org.radarbase.android.auth.oauth2.utils.parser.PreLoginQrParser
import org.radarbase.android.config.SingleRadarConfiguration
import org.radarbase.android.util.ServerConfigUtil.toServerConfig
import org.radarbase.config.ServerConfig
import org.radarbase.producer.AuthenticationException
import org.radarbase.producer.rest.RestClient
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.net.MalformedURLException

/**
 * Authenticates against the RADAR-base ory kratos server.
 */
class OAuth2LoginManager(private val service: AuthService, appAuthState: AppAuthState) : LoginManager, LoginListener {

    private val config: RadarConfiguration = service.radarConfig
    private val sources: MutableMap<String, SourceMetadata> = mutableMapOf()

    private var client: OAuthClient? = null
    private var clientConfig: OAuthClientConfig? = null
    private var restClient: RestClient? = null
    private val configurationObserver: Observer<SingleRadarConfiguration> = Observer { newConfig ->
        refreshOauthClient(newConfig)
    }
    private val stateManager: OAuth2StateManager = OAuth2StateManager(service, client)

    init {
        config.config.observeForever(configurationObserver)
        updateSources(appAuthState)
    }

    override fun refresh(authState: AppAuthState): Boolean {
        if (authState.tokenType != LoginManager.AUTH_TYPE_BEARER) {
            return false
        }
        return authState.getAttribute(LOGIN_REFRESH_TOKEN)
                ?.also { stateManager.refresh(service, it) } != null
    }

    fun parsePreLoginQr(authState: AppAuthState, oAuthQrContent: String) {
        try {
            synchronized(this) {
                val parser = PreLoginQrParser(authState)
                parser.parse(oAuthQrContent).also { appAuth: AppAuthState ->
                    config.updateWithAuthState(service, appAuth)
                    start(appAuth)
                }
            }
        } catch (ex: Exception) {
            logger.error("Failed to authorize with the authorization server: ", ex)
            service.loginFailed(this, ex)
        }
    }


    override fun isRefreshable(authState: AppAuthState): Boolean =
        authState.userId != null && authState.projectId != null && authState.getAttribute(LOGIN_REFRESH_TOKEN) != null

    override fun start(authState: AppAuthState) {
        if (authState.getAttribute(BASE_URL_PROPERTY) == null || authState.projectId == null) {
            logger.debug("Authorization cannot proceed, either the base URL or project ID is missing")
            return
        }
        logger.debug("Staring authorization ")
        service.radarApp.let { app ->
            synchronized(this) {
                stateManager.login(service, app.loginActivity, app.configuration.latestConfig)
            }
        }
    }

    override fun onActivityCreate(activity: Activity): Boolean {
        refreshOauthClient(config.latestConfig)
        val authState: AppAuthState? = stateManager.updateAfterAuthorization(service, activity.intent)
        authState?.let(::updateSources)
        return true
    }

    override fun invalidate(authState: AppAuthState, disableRefresh: Boolean): AppAuthState? {
        return if (authState.authenticationSource != OAUTH2_SOURCE_TYPE) null
        else if (disableRefresh) {
            authState.alter {
                attributes -= LOGIN_REFRESH_TOKEN
                isPrivacyPolicyAccepted = false
            }
        } else {
            authState
        }
    }

    override val sourceTypes: List<String> = OAUTH2_SOURCE_TYPES

    @Throws(AuthenticationException::class)
    override fun registerSource(authState: AppAuthState, source: SourceMetadata,
                       success: (AppAuthState, SourceMetadata) -> Unit,
                       failure: (Exception?) -> Unit): Boolean {
        success(authState, source)
        return true
    }

    @Throws(AuthenticationException::class)
    override fun updateSource(appAuth: AppAuthState, source: SourceMetadata,
                              success: (AppAuthState, SourceMetadata) -> Unit,
                              failure: (Exception?) -> Unit): Boolean {
        success(appAuth, source)
        return true
    }

    override fun onDestroy() {
        stateManager.release()
        config.config.removeObserver(configurationObserver)
    }

    override fun loginSucceeded(manager: LoginManager?, authState: AppAuthState) {
        val token = authState.token
        if (token == null) {
            loginFailed(this,
                    IllegalArgumentException("Cannot login using OAuth2 without a token"))
            return
        }
        try {
            processJwt(authState, Jwt.parse(token)).let {
                service.loginSucceeded(this, it)
            }
        } catch (ex: JSONException) {
            loginFailed(this, ex)
        }

    }

    private fun processJwt(authState: AppAuthState, jwt: Jwt): AppAuthState {
        val body: JSONObject = jwt.body

        return authState.alter {
            authenticationSource = OAUTH2_SOURCE_TYPE
            needsRegisteredSources = true
            expiration = body.optLong("exp", java.lang.Long.MAX_VALUE / 1000L) * 1000L
        }
    }

    @Synchronized
    private fun refreshOauthClient(config: SingleRadarConfiguration) {
        val oAuthClientConfig = try {
            OAuthClientConfig(
                config.getString(MANAGEMENT_PORTAL_URL_KEY),
                config.getBoolean(UNSAFE_KAFKA_CONNECTION, false),
            )
        } catch (e: MalformedURLException) {
            logger.error("Cannot construct OAuthClient with malformed URL")
            null
        } catch (e: Exception) {
            logger.error("Exception when creating oAuth client")
            null
        }

        if (oAuthClientConfig == clientConfig) return

        client = oAuthClientConfig?.let {
            OAuthClient(
                oAuthClientConfig.serverConfig,
                client = restClient,
            ).also { restClient = it.httpClient }
        }
    }

    private fun updateSources(authState: AppAuthState) {
        authState.sourceMetadata
            .forEach { sourceMetadata ->
                sourceMetadata.sourceId?.let {
                    sources[it] = sourceMetadata
                }
            }
    }

    override fun loginFailed(manager: LoginManager?, ex: Exception?) = this.service.loginFailed(this, ex)

    companion object {
        const val OAUTH2_SOURCE_TYPE = "org.radarcns.android.auth.oauth2.OAuth2LoginManager"
        private val OAUTH2_SOURCE_TYPES = listOf(OAUTH2_SOURCE_TYPE)
        const val LOGIN_REFRESH_TOKEN = "org.radarcns.auth.OAuth2LoginManager.refreshToken"
        private val logger: Logger = LoggerFactory.getLogger(OAuth2LoginManager::class.java)
    }

    private data class OAuthClientConfig(val serverConfig: ServerConfig) {
        constructor(url: String, isUnsafeConnection: Boolean): this(url.toServerConfig(isUnsafeConnection))
    }
}
