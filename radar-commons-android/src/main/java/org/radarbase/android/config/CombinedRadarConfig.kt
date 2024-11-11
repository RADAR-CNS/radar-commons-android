package org.radarbase.android.config

import android.content.Context
import androidx.lifecycle.MutableLiveData
import com.google.firebase.analytics.FirebaseAnalytics
import com.google.firebase.crashlytics.FirebaseCrashlytics
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.drop
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.launch
import org.radarbase.android.RadarConfiguration
import org.radarbase.android.RadarConfiguration.Companion.BASE_URL_KEY
import org.radarbase.android.RadarConfiguration.Companion.FETCH_TIMEOUT_MS_DEFAULT
import org.radarbase.android.RadarConfiguration.Companion.FETCH_TIMEOUT_MS_KEY
import org.radarbase.android.RadarConfiguration.Companion.PROJECT_ID_KEY
import org.radarbase.android.RadarConfiguration.Companion.USER_ID_KEY
import org.radarbase.android.RadarConfiguration.RemoteConfigStatus.ERROR
import org.radarbase.android.RadarConfiguration.RemoteConfigStatus.FETCHED
import org.radarbase.android.RadarConfiguration.RemoteConfigStatus.FETCHING
import org.radarbase.android.RadarConfiguration.RemoteConfigStatus.INITIAL
import org.radarbase.android.RadarConfiguration.RemoteConfigStatus.PARTIALLY_FETCHED
import org.radarbase.android.RadarConfiguration.RemoteConfigStatus.READY
import org.radarbase.android.RadarConfiguration.RemoteConfigStatus.UNAVAILABLE
import org.radarbase.android.auth.AppAuthState
import org.radarbase.android.auth.portal.GetSubjectParser.Companion.externalUserId
import org.radarbase.android.auth.portal.GetSubjectParser.Companion.humanReadableUserId
import org.radarbase.kotlin.coroutines.flow.zipAll
import org.radarbase.kotlin.coroutines.launchJoin
import org.slf4j.LoggerFactory
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext

class CombinedRadarConfig(
    private val localConfig: LocalConfig,
    private val remoteConfigs: List<RemoteConfig>,
    defaults: Map<String, String>,
    coroutineContext: CoroutineContext = EmptyCoroutineContext + Dispatchers.Default,
): RadarConfiguration {
    private val defaults = defaults.filterValues { it.isNotEmpty() }

    override val status: MutableStateFlow<RadarConfiguration.RemoteConfigStatus> = MutableStateFlow(INITIAL)
    override var latestConfig: SingleRadarConfiguration
        get() = config.value
        set(value) {
            config.value = value
        }

    override val config: MutableStateFlow<SingleRadarConfiguration> = MutableStateFlow(
        SingleRadarConfiguration(INITIAL, mapOf())
    )

    private val job = SupervisorJob()
    private val configScope = CoroutineScope(coroutineContext + job + CoroutineName("CombinedConfig"))

    init {
        configScope.launch {
            launch {
                updateConfig()
            }
            launch {
                collectStatus()
            }
            launch {
                collectConfig()
            }
            launch {
                propagateConfig()
            }
        }
    }

    private suspend fun collectStatus() {
        remoteConfigs.map { it.status }
            .zipAll()
            .collect { allStatus ->
                status.value = when {
                    FETCHING in allStatus -> FETCHING
                    FETCHED in allStatus && allStatus.all { it == FETCHED || it == UNAVAILABLE } -> FETCHED
                    FETCHED in allStatus || PARTIALLY_FETCHED in allStatus -> PARTIALLY_FETCHED
                    ERROR in allStatus -> ERROR
                    READY in allStatus -> READY
                    INITIAL in allStatus -> INITIAL
                    else -> UNAVAILABLE
                }
            }
    }

    private suspend fun collectConfig() {
        status
            .filter { it == FETCHED }
            .collect { updateConfig(it) }
    }

    private suspend fun propagateConfig() {
        config
            .drop(1)
            .collect { newConfig ->
                remoteConfigs.launchJoin {
                    it.updateWithConfig(newConfig)
                }
            }
    }

    private fun updateConfig(
        status: RadarConfiguration.RemoteConfigStatus = this.status.value,
    ) {
        val newConfig = readConfig(status)
        if (newConfig != latestConfig) {
            if (newConfig.status != latestConfig.status) {
                logger.info("Updating config status to {}", newConfig.status)
            }
            if (newConfig.config != latestConfig.config) {
                logger.info("Updating config to {}", newConfig)
            }
            latestConfig = newConfig
        } else {
            logger.info("No change to config. Skipping.")
        }
    }

    override fun put(key: String, value: Any): String? = localConfig.put(key, value)

    override suspend fun persistChanges() {
        if (localConfig.persistChanges()) {
            updateConfig()
        }
    }

    private fun readConfig(status: RadarConfiguration.RemoteConfigStatus) = SingleRadarConfiguration(status, HashMap<String, String>().apply {
        this += defaults
        remoteConfigs.forEach {
            this += it.cache
        }
        this += localConfig.config
    })

    override suspend fun reset(vararg keys: String) {
        localConfig -= keys
        persistChanges()
    }

    override suspend fun fetch() = remoteConfigs.launchJoin {
        it.fetch(latestConfig.getLong(FETCH_TIMEOUT_MS_KEY, FETCH_TIMEOUT_MS_DEFAULT))
    }

    override suspend fun forceFetch() = remoteConfigs.forEach {
        it.forceFetch()
    }

    override fun toString(): String = latestConfig.toString()

    override suspend fun updateWithAuthState(context: Context, appAuthState: AppAuthState?) {
        val enableAnalytics = appAuthState?.isPrivacyPolicyAccepted == true
        logger.debug("Setting Firebase Analytics enabled: {}", enableAnalytics)
        FirebaseAnalytics.getInstance(context).setAnalyticsCollectionEnabled(enableAnalytics)

        appAuthState ?: return

        val baseUrl = appAuthState.baseUrl
        val projectId = appAuthState.projectId
        val userId = appAuthState.userId
        val crashlytics = FirebaseCrashlytics.getInstance()

        baseUrl?.let {
            put(BASE_URL_KEY, baseUrl)
            put(RadarConfiguration.KAFKA_REST_PROXY_URL_KEY, "$baseUrl/kafka/")
            put(RadarConfiguration.SCHEMA_REGISTRY_URL_KEY, "$baseUrl/schema/")
            put(RadarConfiguration.MANAGEMENT_PORTAL_URL_KEY, "$baseUrl/managementportal/")
            put(RadarConfiguration.OAUTH2_TOKEN_URL, "$baseUrl/managementportal/oauth/token")
            put(RadarConfiguration.OAUTH2_AUTHORIZE_URL, "$baseUrl/managementportal/oauth/authorize")
            crashlytics.setCustomKey(BASE_URL_KEY, baseUrl)
        }

        projectId?.let {
            put(PROJECT_ID_KEY, it)
            crashlytics.setCustomKey(PROJECT_ID_KEY, projectId)
        }
        userId?.let {
            put(USER_ID_KEY, it)
            put(RadarConfiguration.READABLE_USER_ID_KEY, appAuthState.humanReadableUserId ?: it)
            put(RadarConfiguration.EXTERNAL_USER_ID_KEY, appAuthState.externalUserId ?: it)
            crashlytics.setUserId(userId)
            crashlytics.setCustomKey(USER_ID_KEY, userId)
        }

        persistChanges()
        remoteConfigs.forEach {
            it.updateWithAuthState(appAuthState)
        }
        forceFetch()
    }

    suspend fun stop() {
        job.cancelAndJoin()
        remoteConfigs.launchJoin {
            it.stop()
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(CombinedRadarConfig::class.java)
    }
}
