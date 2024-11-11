package org.radarbase.android.auth

import android.content.Intent
import android.content.Intent.*
import android.os.Binder
import android.os.IBinder
import android.os.SystemClock
import androidx.annotation.Keep
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.LifecycleService
import androidx.lifecycle.lifecycleScope
import androidx.lifecycle.repeatOnLifecycle
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.radarbase.android.RadarApplication
import org.radarbase.android.RadarApplication.Companion.radarConfig
import org.radarbase.android.RadarConfiguration
import org.radarbase.android.util.CoroutineTaskExecutor
import org.radarbase.android.util.DelayedRetry
import org.radarbase.android.util.NetworkConnectedReceiver
import org.radarbase.kotlin.coroutines.launchJoin
import org.radarbase.producer.AuthenticationException
import org.slf4j.LoggerFactory
import java.net.ConnectException
import java.util.concurrent.TimeUnit

@Keep
abstract class AuthService : LifecycleService(), LoginListener {

    private val latestAppAuth: AppAuthState
        get() = authState.value

    lateinit var loginManagers: List<LoginManager>
    lateinit var config: RadarConfiguration
    private lateinit var networkConnectedListener: NetworkConnectedReceiver
    private var refreshDelay = DelayedRetry(RETRY_MIN_DELAY, RETRY_MAX_DELAY)
    private var isConnected: Boolean = false
    private val _authState: MutableStateFlow<AppAuthState> = MutableStateFlow(AppAuthState())
    val authState: StateFlow<AppAuthState> = _authState

    private val executor: CoroutineTaskExecutor =
        CoroutineTaskExecutor(AuthService::class.simpleName!!, Dispatchers.Default)

    private val serviceMutex: Mutex = Mutex(false)
    private val registryTweakMutex: Mutex = Mutex(false)
    private val authUpdateMutex: Mutex = Mutex(false)

    private val _authStateFailures: MutableSharedFlow<AuthLoginListener.AuthStateFailure> = MutableSharedFlow(
        extraBufferCapacity = 1,
        onBufferOverflow = BufferOverflow.DROP_OLDEST,
    )

    private val _authStateSuccess: MutableSharedFlow<AuthLoginListener.AuthStateSuccess> = MutableSharedFlow(
        extraBufferCapacity = 1,
        onBufferOverflow = BufferOverflow.DROP_OLDEST
    )

    private val _authStateLogout: MutableSharedFlow<AuthLoginListener.AuthStateLogout> = MutableSharedFlow(
        extraBufferCapacity = 1,
        onBufferOverflow = BufferOverflow.DROP_OLDEST
    )

    private var sourceRegistrationStarted: Boolean = false
    private val authStateFailures: Flow<AuthLoginListener.AuthStateFailure> = _authStateFailures
    private val authStateSuccess: Flow<AuthLoginListener.AuthStateSuccess> = _authStateSuccess
    private val authStateLogout: Flow<AuthLoginListener.AuthStateLogout> = _authStateLogout

    open val authSerialization: AuthSerialization by lazy {
        SharedPreferencesAuthSerialization(this)
    }

    private val successHandlers: MutableList<suspend () -> Unit> = mutableListOf()

    @Volatile
    private var isInLoginActivity: Boolean = false

    private var loginListenerId: Long = 0L
    private val listeners: MutableList<LoginListenerRegistry> = mutableListOf()


    @OptIn(ExperimentalCoroutinesApi::class)
    override fun onCreate() {
        super.onCreate()

        networkConnectedListener = NetworkConnectedReceiver(this)

        executor.start()
        lifecycleScope.launch(Dispatchers.Default) {
            val loadedAuth = authSerialization.load() ?: return@launch
            _authState.value = loadedAuth
            loginManagers = createLoginManagers(loadedAuth)
        }

        lifecycleScope.launch {
            repeatOnLifecycle(Lifecycle.State.STARTED) {
                launch(Dispatchers.Default) {
                    authState.collectLatest { state ->
                        authSerialization.store(state)
                    }
                }
                launch(Dispatchers.Default) {
                    authState.collectLatest { state ->
                        radarConfig.updateWithAuthState(this@AuthService, state)
                    }
                }
                launch(Dispatchers.Default) {
                    networkConnectedListener.monitor()
                    networkConnectedListener.state!!
                        .map { it != NetworkConnectedReceiver.NetworkState.Disconnected }
                        .distinctUntilChanged()
                        .onEach { isConnected = it }
                        .combine(authState) { isConnected, authState ->
                            Pair(isConnected, authState)
                        }
                        .collectLatest { (isConnected, authState) ->
                            if (isConnected && !authState.isValidFor(5, TimeUnit.MINUTES)) {
                                refresh()
                            } else {
                                refreshDelay.reset()
                            }
                        }
                }
                launch(Dispatchers.Default) {
                    authStateFailures
                        .collectLatest { (manager, ex) ->
                            logger.info("Login failed: {}", ex.toString())
                            when (ex) {
                                is AuthenticationException -> {
                                    callListeners {
                                        it.listener.loginFailed(manager, ex)
                                    }
                                }
                                is ConnectException -> {
                                    isConnected = false
                                    executor.delay(refreshDelay.nextDelay(), ::refresh)
                                }
                                else -> {
                                    executor.delay(refreshDelay.nextDelay(), ::refresh)
                                }
                            }
                        }
                }
                launch(Dispatchers.Default) {
                    authStateSuccess
                        .collectLatest { successState: AuthLoginListener.AuthStateSuccess ->
                            logger.info("Log-in succeeded")
                            isConnected = true
                            refreshDelay.reset()
                            callListeners(sinceUpdate = latestAppAuth.lastUpdate) {
                                it.listener.loginSucceeded(successState.manager, latestAppAuth)
                            }
                        }
                }
            }
        }
    }

    /**
     * Refresh the access token. If the existing token is still valid, do not refresh.
     * The request gets handled in a separate thread and returns a result via loginSucceeded. If
     * the current authentication is not valid and it is not possible to refresh the authentication
     * this opens the login activity.
     */
    suspend fun refresh() {
        executor.execute {
            logger.info("Refreshing authentication state")
            if (!authState.value.isValidFor(5, TimeUnit.MINUTES)) {
                doRefresh()
            } else {
                callListeners(sinceUpdate = latestAppAuth.lastUpdate) {
                    it.listener.loginSucceeded(null, latestAppAuth)
                }
            }
        }
    }

    private suspend fun doRefresh() {
        var canRefresh = false
        updateState<Unit> { state ->
            canRefresh = state.relevantManagers.any { it.refresh(this) }
        }
        if (!canRefresh) {
            startLogin()
        }
    }

    /**
     * Refresh the access token. If the existing token is still valid, do not refresh.
     * The request gets handled in a separate thread and returns a result via loginSucceeded. If
     * there is no network connectivity, just check if the authentication could be refreshed if the
     * client was online.
     */
    suspend fun refreshIfOnline() {
        executor.execute {
            if (isConnected) {
                refresh()
            } else {
                updateState { appAuth ->
                    if (appAuth.isValid || appAuth.relevantManagers.any {
                            it.isRefreshable(
                                appAuth
                            )
                        }) {
                        logger.info("Retrieving active authentication state without refreshing")
                        callListeners(sinceUpdate = appAuth.lastUpdate) {
                            it.listener.loginSucceeded(null, appAuth)
                        }
                    } else {
                        logger.error(
                            "Failed to retrieve authentication state without refreshing",
                            IllegalStateException(
                                "Cannot refresh authentication state $appAuth: not online and no" +
                                        "applicable authentication manager."
                            )
                        )
                        startLogin()
                    }
                }
            }
        }
    }

    protected open fun startLogin() {
        if (!isInLoginActivity) {
            try {
                logger.info("Starting login activity")
                val intent = Intent(this, (application as RadarApplication).loginActivity)
                intent.flags = FLAG_ACTIVITY_NEW_TASK or FLAG_ACTIVITY_TASK_ON_HOME or FLAG_ACTIVITY_CLEAR_TOP or FLAG_ACTIVITY_SINGLE_TOP
                startActivity(intent)
            } catch (ex: IllegalStateException) {
                logger.warn("Failed to start login activity. Notifying about login.")
                showLoginNotification()
            }
        }
    }

    private suspend fun callListeners(call: suspend (LoginListenerRegistry) -> Unit) {
        serviceMutex.withLock {
            listeners.launchJoin {
                call(it)
            }
        }
    }

    private suspend fun callListeners(sinceUpdate: Long, call: (LoginListenerRegistry) -> Unit) {
        serviceMutex.withLock {
            val obsoleteListeners = listeners.filter { it.lastUpdated < sinceUpdate }
            obsoleteListeners.launchJoin {
                it.lastUpdated = sinceUpdate
                call(it)
            }
        }
    }

    suspend fun addLoginListener(loginListener: LoginListener): LoginListenerRegistry {
        executor.execute {
            updateState { auth ->
                if (auth.isValid) {
                    loginListener.loginSucceeded(null, auth)
                }
            }
        }

        return LoginListenerRegistry(loginListener)
            .also {
                registryTweakMutex.withLock { listeners += it }
            }
    }

    suspend fun removeLoginListener(registry: LoginListenerRegistry) {
        registryTweakMutex.withLock {
            logger.debug(
                "Removing login listener #{}: {} (starting with {} listeners)",
                registry.id,
                registry.listener,
                listeners.size
            )
            listeners -= listeners.filter { it.id == registry.id }.toSet()
        }
    }


    protected abstract fun showLoginNotification()

    override fun loginFailed(manager: LoginManager?, ex: java.lang.Exception?) {
        _authStateFailures.tryEmit(AuthLoginListener.AuthStateFailure(manager, ex))
    }

    override fun loginSucceeded(manager: LoginManager?, authState: AppAuthState) {
        _authStateSuccess.tryEmit(AuthLoginListener.AuthStateSuccess(manager))
        _authState.value = authState
    }

    override fun logoutSucceeded(manager: LoginManager?, authState: AppAuthState) {
        _authStateLogout.tryEmit(AuthLoginListener.AuthStateLogout(manager))
        _authState.value = authState
    }

    private val AppAuthState.relevantManagers: List<LoginManager>
        get() = loginManagers.filter { it.appliesTo(this) }

    private val AppAuthState.manager: LoginManager
        get() = relevantManagers.first()

    override fun onDestroy() {
        super.onDestroy()
        loginManagers.forEach { it.onDestroy() }
        executor.stop()
    }

    suspend fun update(manager: LoginManager) {
        logger.info("Refreshing manager {}", manager)
        updateState {
            manager.refresh(this)
        }
    }

    /**
     * Create your login managers here. Call [LoginManager.start] for the login method that
     * a user indicates.
     * @param appAuth previous invalid authentication
     * @return non-empty list of login managers to use
     */
    protected abstract suspend fun createLoginManagers(appAuth: AppAuthState): List<LoginManager>

    private suspend fun <T> updateState(
        update: suspend AppAuthState.Builder.(AppAuthState) -> T
    ): T {
        logger.info("Trying to tweak auth state")
        authUpdateMutex.withLock {
            var result: T? = null
            do {
                logger.debug("Updating auth state")
                val currentValue = authState.value
                val newValue = currentValue.alter {
                    result = this.update(currentValue)
                }
            } while (!_authState.compareAndSet(currentValue, newValue))
            @Suppress("UNCHECKED_CAST")
            return result as T
        }
    }

    private fun applyState(function: AppAuthState.() -> Unit) {
        val updatedState = latestAppAuth.apply(function)
        _authState.value = updatedState
    }

    suspend fun invalidate(token: String?, disableRefresh: Boolean) {
        executor.execute {
            updateState { auth ->
                logger.info("Invalidating authentication state")
                if (token != null && token != auth.token) return@updateState

                auth.relevantManagers.forEach {
                    it.invalidate(this@updateState, disableRefresh)
                }
                if (disableRefresh) {
                    clear()
                } else {
                    invalidate()
                }
            }
        }
    }

    suspend fun registerSource(
        source: SourceMetadata,
        success: suspend (AppAuthState, SourceMetadata) -> Unit,
        failure: suspend (Exception?) -> Unit
    ) {
        logger.info(
            "Registering source with SourceType: {}, and SourceId: {}",
            source.type,
            source.sourceId
        )

        updateState { auth ->
            auth.relevantManagers.any { manager ->
                manager.registerSource(
                    this,
                    source,
                    { newAppAuth, newSource ->
                        if (newAppAuth != auth) {
                            _authState.value = newAppAuth
                        }
                        successHandlers += { success(newAppAuth, newSource) }
                        if (!sourceRegistrationStarted) {
                            executor.delay(1_000L) {
                                doRefresh()
                                successHandlers.forEach { it() }
                                successHandlers.clear()
                                sourceRegistrationStarted = false
                            }
                            sourceRegistrationStarted = true
                        }
                    },
                    { ex ->
                        updateState {
                            sourceMetadata.removeAll(source::matches)
                        }
                        authSerialization.store(auth)
                        failure(ex)
                    })
            }
        }
    }

    override fun onBind(intent: Intent): IBinder? {
        super.onBind(intent)
        return AuthServiceBinder()
    }

    inner class AuthServiceBinder: Binder() {

        suspend fun addLoginListener(loginListener: LoginListener) =
            this@AuthService.addLoginListener(loginListener)

        suspend fun removeLoginListener(registry: LoginListenerRegistry) =
            this@AuthService.removeLoginListener(registry)

        val managers: List<LoginManager>
            get() = loginManagers

        val authStateFailures: Flow<AuthLoginListener.AuthStateFailure>
            get() = this@AuthService.authStateFailures

        val authStateSuccess: Flow<AuthLoginListener.AuthStateSuccess>
            get() = this@AuthService.authStateSuccess

        suspend fun update(manager: LoginManager) = this@AuthService.update(manager)

        suspend fun refresh() = this@AuthService.refresh()

        suspend fun refreshIfOnline() = this@AuthService.refreshIfOnline()

        fun applyState(apply: AppAuthState.() -> Unit) = this@AuthService.applyState(apply)

        @Suppress("unused")
        suspend fun <T> updateState(update: AppAuthState.Builder.(AppAuthState) -> T): T = this@AuthService.updateState(update)

        suspend fun registerSource(
            source: SourceMetadata,
            success: suspend (AppAuthState, SourceMetadata) -> Unit, failure: (Exception?) -> Unit
        ) =
            this@AuthService.registerSource(source, success, failure)

        fun updateSource(source: SourceMetadata, success: (AppAuthState, SourceMetadata) -> Unit, failure: (Exception?) -> Unit) =
                this@AuthService.updateSource(source, success, failure)

        suspend fun unregisterSources(sources: Iterable<SourceMetadata>) =
                this@AuthService.unregisterSources(sources)

        suspend fun invalidate(token: String?, disableRefresh: Boolean) = this@AuthService.invalidate(token, disableRefresh)

        var isInLoginActivity: Boolean
            get() = this@AuthService.isInLoginActivity
            set(value) {
                this@AuthService.isInLoginActivity = value
            }
    }

    private fun updateSource(
        source: SourceMetadata,
        success: (AppAuthState, SourceMetadata) -> Unit,
        failure: (Exception?) -> Unit
    ) {
        executor.execute {
            updateState {
                it.relevantManagers.any { manager ->
                    manager.updateSource(this, source, success, failure)
                }
            }
        }
        loginManagers
    }

    sealed interface AuthLoginListener {
        data class AuthStateFailure(val manager: LoginManager?, val exception: Exception?) :
            AuthLoginListener

        data class AuthStateSuccess(
            val manager: LoginManager? = null
        ) : AuthLoginListener

        data class AuthStateLogout(val manager: LoginManager?) :
            AuthLoginListener
    }

    private suspend fun unregisterSources(sources: Iterable<SourceMetadata>) {
        updateState {
            sourceMetadata -= sources.toSet()
        }
        doRefresh()
    }

    inner class LoginListenerRegistry(val listener: LoginListener) {
        val id = ++loginListenerId
        var lastUpdated = 0L
    }

    companion object {
        private val logger = LoggerFactory.getLogger(AuthService::class.java)
        const val RETRY_MIN_DELAY = 5L
        const val RETRY_MAX_DELAY = 86400L
        const val PRIVACY_POLICY_URL_PROPERTY = "org.radarcns.android.auth.portal.ManagementPortalClient.privacyPolicyUrl"
        const val BASE_URL_PROPERTY = "org.radarcns.android.auth.portal.ManagementPortalClient.baseUrl"
    }
}
