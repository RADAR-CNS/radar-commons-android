package org.radarbase.android.config

import android.content.Context
import android.content.Context.MODE_PRIVATE
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.request.*
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.drop
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import org.radarbase.android.RadarConfiguration
import org.radarbase.android.RadarConfiguration.Companion.BASE_URL_KEY
import org.radarbase.android.RadarConfiguration.Companion.OAUTH2_CLIENT_ID
import org.radarbase.android.RadarConfiguration.Companion.UNSAFE_KAFKA_CONNECTION
import org.radarbase.android.auth.AppAuthState
import org.radarbase.android.util.DelayedRetry
import org.radarbase.android.util.ServerConfigUtil.toServerConfig
import org.radarbase.appconfig.api.ClientConfig
import org.radarbase.config.ServerConfig
import org.radarbase.producer.io.timeout
import org.radarbase.producer.io.unsafeSsl
import org.slf4j.LoggerFactory
import java.io.IOException
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.time.Duration.Companion.seconds

@Suppress("unused")
class AppConfigRadarConfiguration(
    context: Context,
    coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : RemoteConfig {
    private var auth: AppAuthState? = null
    private var config: SingleRadarConfiguration? = null
    private val retryDelay = DelayedRetry(minDelay = 10_000L, maxDelay = 3_600_000L)
    private val job = SupervisorJob()
    private val appconfigScope = CoroutineScope(coroutineContext + job)

    @get:Synchronized
    private var appConfig: AppConfigClientConfig? = null

    override var status = MutableStateFlow(RadarConfiguration.RemoteConfigStatus.INITIAL)

    private val preferences = context.getSharedPreferences(
        "org.radarbase.android.config.AppConfigRadarConfiguration",
        MODE_PRIVATE
    )

    @Suppress("UNCHECKED_CAST")
    @get:Synchronized
    @set:Synchronized
    override var cache: Map<String, String> = preferences.all.filterValues { it is String } as Map<String, String>
        private set

    @Volatile
    override var lastFetch: Long = 0L

    private var client: HttpClient? = null

    private val configMutex = Mutex()

    init {
        lastFetch = preferences.getLong(LAST_FETCH, 0L)
        val storageIsAlreadyFetched = lastFetch != 0L
        if (storageIsAlreadyFetched) {
            status.value = RadarConfiguration.RemoteConfigStatus.FETCHED
        }
        appconfigScope.launch {
            val statusFlow = if (storageIsAlreadyFetched) status.drop(1) else status
            statusFlow
                .filter { it == RadarConfiguration.RemoteConfigStatus.FETCHED }
                .collect { storeCache() }
        }
    }

    override suspend fun doFetch(maxCacheAge: Long) {
        val (client, appConfig) = configMutex.withLock {
            Pair(client, appConfig)
        }
        if (appConfig == null || client == null) {
            logger.warn("Cannot fetch configuration without AppConfig client configuration")
            status.value = RadarConfiguration.RemoteConfigStatus.UNAVAILABLE
            return
        }

        status.value = RadarConfiguration.RemoteConfigStatus.FETCHING

        val statement = client.prepareRequest("projects/${appConfig.appAuthState.projectId}/users/${appConfig.appAuthState.userId}/clients/${appConfig.clientId}") {
            headers {
                appendAll(appConfig.appAuthState.ktorHeaders)
            }
        }

        status.value = withContext(Dispatchers.IO) {
            try {
                val response = statement.execute()
                when {
                    response.status.isSuccess() -> {
                        val body: ClientConfig = response.body()

                        cache = buildMap {
                            body.defaults?.forEach {
                                val value = it.value ?: return@forEach
                                put(it.name, value)
                            }
                            body.config.forEach {
                                val value = it.value
                                if (value.isNullOrEmpty()) {
                                    remove(it.name)
                                } else {
                                    put(it.name, value)
                                }
                            }
                        }
                        RadarConfiguration.RemoteConfigStatus.FETCHED
                    }
                    response.status == HttpStatusCode.NotFound -> {
                        logger.warn("AppConfig service not found")
                        RadarConfiguration.RemoteConfigStatus.UNAVAILABLE
                    }
                    else -> throw IOException("AppConfig update request failed with code ${response.status}")
                }
            } catch (ex: Exception) {
                logger.error("AppConfig update request failed", ex)
                retryLater(appConfig)
                RadarConfiguration.RemoteConfigStatus.ERROR
            }
        }
    }

    private suspend fun storeCache() {
        withContext(Dispatchers.IO) {
            preferences.edit().apply {
                cache.entries.forEach { (k, v) ->
                    putString(k, v)
                }
                putLong(LAST_FETCH, lastFetch)
            }.commit()
        }
    }

    private suspend fun retryLater(appConfig: AppConfigClientConfig) {
        appconfigScope.launch {
            delay(retryDelay.nextDelay())
            fetch(appConfig.fetchTimeout)
        }
    }

    override suspend fun updateWithAuthState(appAuthState: AppAuthState?) {
        configMutex.withLock {
            auth = appAuthState
            updateConfiguration(appAuthState, config)
        }
    }

    override suspend fun updateWithConfig(config: SingleRadarConfiguration?) {
        configMutex.withLock {
            this.config = config
            updateConfiguration(auth, config)
        }
    }

    private suspend fun updateConfiguration(auth: AppAuthState?, config: SingleRadarConfiguration?) {
        val appConfig = config?.let { value ->
            val serverConfig = value.optString(BASE_URL_KEY) { baseUrl ->
                "$baseUrl/appconfig/api/".toServerConfig(
                    isUnsafe = value.getBoolean(UNSAFE_KAFKA_CONNECTION, false),
                )
            }
            val clientId = value.optString(OAUTH2_CLIENT_ID)
            val timeout = config.getLong(RadarConfiguration.FETCH_TIMEOUT_MS_KEY, RadarConfiguration.FETCH_TIMEOUT_MS_DEFAULT)
            if (auth != null && auth.isValid && serverConfig != null && clientId != null) {
                AppConfigClientConfig(auth, serverConfig, clientId, timeout)
            } else null
        }
        if (appConfig == this.appConfig) {
            return
        }
        logger.info("AppConfig config {}", appConfig)
        this.appConfig = appConfig
        if (appConfig != null) {
            status.value = RadarConfiguration.RemoteConfigStatus.READY
            client = baseClient.config {
                defaultRequest {
                    url(appConfig.serverConfig.urlString)
                    headers {
                        appendAll(appConfig.appAuthState.ktorHeaders)
                    }
                    accept(ContentType.Application.Json)
                }
                if (appConfig.serverConfig.isUnsafe) {
                    unsafeSsl()
                }
            }
            fetch(appConfig.fetchTimeout)
        } else {
            status.value = RadarConfiguration.RemoteConfigStatus.UNAVAILABLE
            client = null
        }
    }

    override suspend fun stop() {
        job.cancelAndJoin()
        baseClient.close()
    }

    private data class AppConfigClientConfig(
        val appAuthState: AppAuthState,
        val serverConfig: ServerConfig,
        val clientId: String,
        val fetchTimeout: Long,
    )

    companion object {
        private val logger = LoggerFactory.getLogger(AppConfigRadarConfiguration::class.java)
        private const val LAST_FETCH = "org.radarbase.android.config.AppConfigRadarConfiguration.lastFetch"


        private val baseClient: HttpClient = HttpClient(CIO) {
            install(ContentNegotiation) {
                json(Json {
                    ignoreUnknownKeys = true
                })
            }
            timeout(10.seconds)
        }
    }
}
