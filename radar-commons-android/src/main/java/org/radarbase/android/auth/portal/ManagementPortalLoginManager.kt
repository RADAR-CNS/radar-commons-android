package org.radarbase.android.auth.portal

import android.app.Activity
import io.ktor.client.*
import io.ktor.client.engine.cio.CIO
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.json.JSONException
import org.radarbase.android.RadarApplication.Companion.radarConfig
import org.radarbase.android.RadarConfiguration.Companion.MANAGEMENT_PORTAL_URL_KEY
import org.radarbase.android.RadarConfiguration.Companion.OAUTH2_CLIENT_ID
import org.radarbase.android.RadarConfiguration.Companion.OAUTH2_CLIENT_SECRET
import org.radarbase.android.RadarConfiguration.Companion.UNSAFE_KAFKA_CONNECTION
import org.radarbase.android.auth.AppAuthState
import org.radarbase.android.auth.AuthService
import org.radarbase.android.auth.LoginManager
import org.radarbase.android.auth.SourceMetadata
import org.radarbase.android.auth.portal.ManagementPortalClient.Companion.MP_REFRESH_TOKEN_PROPERTY
import org.radarbase.android.config.SingleRadarConfiguration
import org.radarbase.android.util.ServerConfigUtil.toServerConfig
import org.radarbase.config.ServerConfig
import org.radarbase.producer.AuthenticationException
import org.radarbase.producer.io.timeout
import org.radarbase.producer.io.unsafeSsl
import org.slf4j.LoggerFactory
import java.io.IOException
import java.net.MalformedURLException
import kotlin.time.Duration.Companion.seconds

/**
 * Manages login and token refresh operations with the Management Portal.
 * This class is responsible for handling authentication state, managing OAuth tokens, and
 * maintaining source metadata associated with a user or subject.
 *
 * It communicates with the Management Portal API using the Ktor HTTP client, enabling registration,
 * token refresh, and source management operations. It ensures thread safety with `Mutex` during
 * token refreshes or client interaction.
 *
 * @property listener the AuthService that listens for login and authentication state changes
 * @property authState a StateFlow object that represents the current authentication state
 */
class ManagementPortalLoginManager(
    private val listener: AuthService,
    private val authState: StateFlow<AppAuthState>
) : LoginManager {
    private val sources: MutableMap<String, SourceMetadata> = mutableMapOf()

    private var client: ManagementPortalClient? = null
    private var ktorClient: HttpClient? = null
    private var clientConfig: ManagementPortalConfig? = null
    private val config = listener.radarConfig
    private val mutex: Mutex = Mutex()

    private val mpManagerExceptionHandler = CoroutineExceptionHandler { _, throwable ->
        logger.error("Exception while ensuring client connectivity", throwable)
    }
    private var configObserverJob: Job? = null
    private val managerScope = CoroutineScope(Job() + mpManagerExceptionHandler + CoroutineName("mpManagerScope") + Dispatchers.Default)

    init {
        ktorClient = HttpClient(CIO)
        configObserverJob = managerScope.launch {
            config.config
                .collect { newConfig: SingleRadarConfiguration ->
                    ensureClientConnectivity(newConfig)
                }
        }
        updateSources(authState.value)
    }

    /**
     * Sets a new refresh token and updates the current authentication state.
     *
     * @param authState the authentication state to update
     * @param refreshToken the new refresh token to be set
     */
    suspend fun setRefreshToken(authState: AppAuthState.Builder, refreshToken: String) {
        refresh(authState.apply { attributes[MP_REFRESH_TOKEN_PROPERTY] = refreshToken })
    }

    /**
     * Sets the refresh token from the given meta-token URL and updates the authentication state accordingly.
     *
     * @param authState the authentication state to update
     * @param refreshTokenUrl the URL from which to retrieve the refresh token
     */
    suspend fun setTokenFromUrl(authState: AppAuthState.Builder, refreshTokenUrl: String) {
        client?.let { client ->
            mutex.withLock {
                try {
                    // create parser
                    val parser = MetaTokenParser(authState)

                    // retrieve token and update authState
                    client.getRefreshToken(refreshTokenUrl, parser).let { authState ->
                        // update radarConfig
                        config.updateWithAuthState(listener, authState.build())
                        // refresh client
                        ensureClientConnectivity(config.latestConfig)
                        logger.info("Retrieved refreshToken from url")
                        // refresh token
                        refresh(authState)
                    }
                } catch (ex: Exception) {
                    logger.error("Failed to get meta token", ex)
                    listener.loginFailed(this, ex)
                }
            }
        }
    }

    override suspend fun refresh(authState: AppAuthState.Builder): Boolean {
        authState.attributes[MP_REFRESH_TOKEN_PROPERTY] ?: return false
        val client = client ?: return true
        mutex.withLock {
            try {
                val subjectParser = SubjectTokenParser(client, authState)

                client.refreshToken(authState, subjectParser).let {
                    logger.info("Refreshed JWT")

                    val appAuth = authState.build()
                    updateSources(appAuth)
                    listener.loginSucceeded(this, appAuth)
                }
            } catch (exception: Exception) {
                logger.error("Failed to receive access token", exception)
                throw exception
            }
        }
        return true
    }

    override fun isRefreshable(authState: AppAuthState): Boolean {
        return authState.userId != null
                && authState.projectId != null
                && authState.getAttribute(MP_REFRESH_TOKEN_PROPERTY) != null
    }

    override suspend fun start(authState: AppAuthState) {
        refresh(AppAuthState.Builder(authState))
    }

    override fun onActivityCreate(activity: Activity): Boolean {
        return false
    }

    override suspend fun invalidate(authState: AppAuthState.Builder, disableRefresh: Boolean) {
        if (disableRefresh) {
            authState.clear()
        }
    }

    override val sourceTypes: Set<String> = sourceTypeList

    override suspend fun updateSource(
        appAuth: AppAuthState.Builder,
        source: SourceMetadata,
        success: (AppAuthState, SourceMetadata) -> Unit,
        failure: (Exception?) -> Unit
    ): Boolean {
        logger.debug("Handling source update")

        val client = client

        if (client != null) {
            try {
                val updatedSource = client.updateSource(appAuth.build(), source)
                success(appAuth.addSource(updatedSource), updatedSource)
            } catch (ex: UnsupportedOperationException) {
                logger.warn("ManagementPortal does not support updating the app source.")
                success(appAuth.addSource(source), source)
            } catch (ex: ManagementPortalClient.Companion.SourceNotFoundException) {
                logger.warn("Source no longer exists - removing from auth state")
                appAuth.removeSource(source)
                registerSource(appAuth, source, success, failure)
            } catch (ex: ManagementPortalClient.Companion.UserNotFoundException) {
                logger.warn("User no longer exists - invalidating auth state")
                listener.invalidate(appAuth.token, false)
                failure(ex)
            } catch (ex: java.lang.IllegalArgumentException) {
                logger.error("Source {} is not valid", source)
                failure(ex)
            } catch (ex: AuthenticationException) {
                listener.invalidate(appAuth.token, false)
                logger.error("Authentication error; failed to update source {} of type {}",
                    source.sourceName, source.type, ex)
                failure(ex)
            } catch (ex: IOException) {
                logger.error("Failed to update source {} with {}",
                    source.sourceName, source.type, ex)
                failure(ex)
            } catch (ex: JSONException) {
                logger.error("Failed to update source {} with {}",
                    source.sourceName, source.type, ex)
                failure(ex)
            }
        } else {
            failure(IllegalStateException("Cannot update source without a client"))
        }

        return true
    }

    override suspend fun registerSource(
        authState: AppAuthState.Builder,
        source: SourceMetadata,
        success: suspend (AppAuthState, SourceMetadata) -> Unit,
        failure: suspend (Exception?) -> Unit
    ): Boolean {
        logger.debug("Handling source registration")
        val appAuth: AppAuthState = authState.build()
        val existingSource = sources[source.sourceId]
        if (existingSource != null) {
            success(appAuth, source)
            return true
        }

        val client = client
        if (client == null) {
            failure(IllegalStateException("Cannot register source without a client"))
            return false
        }

        if (authState.original == null)  {
            failure(IllegalStateException("Cannot register source without a base authentication."))
            return false
        }

        try {
            val updatedSource = if (source.sourceId == null) {
                // temporary measure to reuse existing source IDs if they exist
                source.type?.id
                        ?.let { sourceType -> sources.values.find { it.type?.id == sourceType } }
                        ?.let {
                            source.sourceId = it.sourceId
                            source.sourceName = it.sourceName
                            client.updateSource(authState.original, source)
                        }
                        ?: client.registerSource(authState.original, source)
            } else {
                client.updateSource(authState.original, source)
            }
            success(authState.addSource(updatedSource), updatedSource)
        } catch (ex: UnsupportedOperationException) {
            logger.warn("ManagementPortal does not support updating the app source")
            success(authState.addSource(source),source)
        } catch (ex: ManagementPortalClient.Companion.SourceNotFoundException) {
            logger.warn("Source no longer exists - removing from auth state.")
            authState.removeSource(source)
            registerSource(authState, source, success, failure)
        } catch (ex: ManagementPortalClient.Companion.UserNotFoundException) {
            logger.warn("User no longer exists - invalidating auth state.")
            listener.invalidate(authState.token, false)
            failure(ex)
        } catch (ex: ConflictException) {
            try {
                client.getSubject(authState, GetSubjectParser(authState)).let { appAuthState ->
                    updateSources(appAuthState.build())
                    sources[source.sourceId]?.let { sourceMetadata ->
                        success(authState.build(), sourceMetadata)
                    } ?:
                    failure(IllegalStateException("Source was not added to ManagementPortal, even though conflict was reported."))
                }
            } catch (ioEx: IOException) {
                logger.error("Failed to register source {} with {}: already registered",
                        source.sourceName, source.type, ex)
                failure(ex)
            }
        } catch (ex: java.lang.IllegalArgumentException) {
            logger.error("Source {} is not valid.", source)
            failure(ex)
        } catch (ex: AuthenticationException) {
            listener.invalidate(authState.token, false)
            logger.error("Authentication error; failed to register source {} of type {}",
                    source.sourceName, source.type, ex)
            failure(ex)
        } catch (ex: IOException) {
            logger.error("Failed to register source {} with {}",
                    source.sourceName, source.type, ex)
            failure(ex)
        } catch (ex: JSONException) {
            logger.error("Failed to register source {} with {}",
                    source.sourceName, source.type, ex)
            failure(ex)
        }

        return true
    }


    private fun updateSources(authState: AppAuthState) {
        authState.sourceMetadata
            .forEach { sourceMetadata ->
                sourceMetadata.sourceId?.let {
                    sources[it] = sourceMetadata
                }
            }
    }

    override fun onDestroy() {
        configObserverJob?.cancel()
        ktorClient = ktorClient?.let {
            it.close()
            null
        }
        managerScope.cancel()
    }

    /**
     * Ensures that the client is correctly configured and connected to the Management Portal.
     *
     * This method checks if the configuration has changed, and if it has, it reconfigures the
     * `HttpClient` instance with the new server settings. The client configuration includes setting
     * the OAuth client ID, client secret, and server URL.
     *
     * It also applies any necessary SSL settings, including unsafe connections for development or
     * testing purposes.
     *
     * @param config the updated firebase configuration from which the ManagementPortal settings will be extracted.
     */
    @Synchronized
    private fun ensureClientConnectivity(config: SingleRadarConfiguration) {

        val currentClient = ktorClient ?: run {
            logger.warn("Ktor client turned null unexpectedly, not proceeding further")
            return
        }

        val newClientConfig = try {
            ManagementPortalConfig(
                config.getString(MANAGEMENT_PORTAL_URL_KEY),
                config.getBoolean(UNSAFE_KAFKA_CONNECTION, false),
                config.getString(OAUTH2_CLIENT_ID),
                config.getString(OAUTH2_CLIENT_SECRET, ""),
            )
        } catch (e: MalformedURLException) {
            logger.error("Cannot construct ManagementPortalClient with malformed URL")
            null
        } catch (e: IllegalArgumentException) {
            logger.error("Cannot construct ManagementPortalClient without client credentials")
            null
        }

        if (newClientConfig == clientConfig) return

        newClientConfig?.let { clientConfig ->
            currentClient.config {
                if (clientConfig.serverConfig.isUnsafe) {
                    unsafeSsl()
                }
                timeout(30.seconds)
            }
        }

        client = newClientConfig?.let {
            ManagementPortalClient(
                newClientConfig.serverConfig,
                config.getString(OAUTH2_CLIENT_ID),
                config.getString(OAUTH2_CLIENT_SECRET, ""),
                client = currentClient,
            )
        }
    }

    /**
     * Adds a new source to the [AppAuthState] and updates the local source metadata.
     *
     * This method updates the sourceMetadata in the provided authentication state by adding the
     * new source. If a source with the same ID already exists, it is replaced. This method also
     * ensures that the source is valid by checking its ID before adding it to the state.
     *
     * @param source the `SourceMetadata` representing the new source to be added.
     * @return the updated `AppAuthState` after the source has been added.
     */
    private fun AppAuthState.Builder.addSource(source: SourceMetadata): AppAuthState {
        val sourceId = source.sourceId
        return if (sourceId == null) {
            logger.error("Cannot add source {} without ID", source)
            build()
        } else {
            sources[sourceId] = source

            val containedPreviousSource = sourceMetadata.removeAll { it.sourceId == sourceId }
            if (!containedPreviousSource) {
                invalidate()
            }
            sourceMetadata += source
            build()
        }
    }

    /**
     * Removes a source from the [AppAuthState] and updates the local source metadata.
     *
     * This method removes the specified source from the authentication state and updates the local
     * `sources` map accordingly. It also invalidates the state if the source is successfully removed.
     *
     * @param source the `SourceMetadata` representing the source to be removed.
     * @return the updated `AppAuthState` after the source has been removed.
     */
    private fun AppAuthState.Builder.removeSource(source: SourceMetadata): AppAuthState {
        sources.remove(source.sourceId)
        val containedPreviousSource = sourceMetadata.removeAll { it.sourceId == source.sourceId }
        if (containedPreviousSource) {
            invalidate()
        }
        source.sourceId = null
        return build()
    }

    companion object {
        const val SOURCE_TYPE = "org.radarcns.auth.portal.ManagementPortal"
        private val logger = LoggerFactory.getLogger(ManagementPortalLoginManager::class.java)
        val sourceTypeList = setOf(SOURCE_TYPE)
    }

    /**
     * Data class representing the configuration needed to communicate with the Management Portal.
     *
     * This configuration includes the server URL, client ID, and client secret, as well as whether
     * the connection to the server should be considered unsafe (e.g., for development purposes).
     *
     * @property serverConfig the `ServerConfig` containing the URL and security settings for the Management Portal.
     * @property clientId the OAuth2 client ID used to authenticate the application with the Management Portal.
     * @property clientSecret the OAuth2 client secret used in combination with the client ID for authentication.
     */
    private data class ManagementPortalConfig(
        val serverConfig: ServerConfig,
        val clientId: String,
        val clientSecret: String,
    ) {
        constructor(
            url: String,
            unsafe: Boolean,
            clientId: String,
            clientSecret: String,
        ) : this(
            url.toServerConfig(unsafe),
            clientId,
            clientSecret
        )
    }
}
