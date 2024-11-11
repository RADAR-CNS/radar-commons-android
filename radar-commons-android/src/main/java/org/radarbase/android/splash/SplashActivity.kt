package org.radarbase.android.splash

import android.app.Activity
import android.content.Intent
import android.content.Intent.FLAG_ACTIVITY_CLEAR_TOP
import android.content.Intent.FLAG_ACTIVITY_NEW_TASK
import android.content.Intent.FLAG_ACTIVITY_SINGLE_TOP
import android.content.Intent.FLAG_ACTIVITY_TASK_ON_HOME
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.os.SystemClock
import androidx.annotation.CallSuper
import androidx.annotation.Keep
import androidx.appcompat.app.AppCompatActivity
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.lifecycleScope
import androidx.lifecycle.repeatOnLifecycle
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.radarbase.android.RadarApplication.Companion.radarApp
import org.radarbase.android.RadarApplication.Companion.radarConfig
import org.radarbase.android.RadarConfiguration
import org.radarbase.android.auth.AppAuthState
import org.radarbase.android.auth.AuthService
import org.radarbase.android.auth.AuthServiceStateReactor
import org.radarbase.android.auth.LoginListener
import org.radarbase.android.auth.LoginManager
import org.radarbase.android.util.BindState
import org.radarbase.android.util.ManagedServiceConnection
import org.radarbase.android.util.ManagedServiceConnection.Companion.serviceConnection
import org.radarbase.android.util.NetworkConnectedReceiver
import org.radarbase.kotlin.coroutines.launchJoin
import org.radarbase.producer.AuthenticationException
import org.slf4j.LoggerFactory
import java.io.IOException

/**
 * Ensure that settings and authentication is loaded
 */
@Keep
abstract class SplashActivity : AppCompatActivity() {
    private lateinit var authServiceConnection: ManagedServiceConnection<AuthService.AuthServiceBinder>
    private lateinit var loginListener: LoginListener
    private lateinit var config: RadarConfiguration
    private lateinit var networkReceiver: NetworkConnectedReceiver

    protected var configReceiver: Boolean = false

    protected var state: Int = STATE_INITIAL
    protected abstract val delayMs: Long
    protected var startedAt: Long = 0
    protected var enable = true
    protected var waitForFullFetchMs = 0L

    protected lateinit var handler: Handler
    protected var startActivityFuture: Job? = null
    private var listenerRegistry: AuthService.LoginListenerRegistry? = null
    private var authSplashBinder: AuthService.AuthServiceBinder? = null

    private val splashServiceBoundActions: MutableList<AuthServiceStateReactor> = mutableListOf(
        {binder -> binder.refreshIfOnline()},
        { binder ->
            listenerRegistry = binder.addLoginListener(loginListener)
        }
    )

    private val splashUnboundActions: MutableList<AuthServiceStateReactor> = mutableListOf(
        { binder ->
            listenerRegistry?.let {
                binder.removeLoginListener(it)
                listenerRegistry = null
            }
        }
    )

    private var configCollectorJob: Job? = null

    private suspend fun updateConfig(status: RadarConfiguration.RemoteConfigStatus, allowPartialConfiguration: Boolean) {
        if (enable
            && (lifecycle.currentState == Lifecycle.State.RESUMED
                    || lifecycle.currentState == Lifecycle.State.STARTED)
            && state != STATE_AUTHORIZING
        ) {
            if (
                status == RadarConfiguration.RemoteConfigStatus.FETCHED
                || (status == RadarConfiguration.RemoteConfigStatus.PARTIALLY_FETCHED
                        && (waitForFullFetchMs <= 0L || allowPartialConfiguration))
            ) {
                logger.info("Config has been fetched, checking authentication")
                stopConfigListener()
                startAuthConnection()
            } else if (status == RadarConfiguration.RemoteConfigStatus.PARTIALLY_FETCHED) {

                lifecycleScope.launch {
                    delay(waitForFullFetchMs)
                    updateConfig(radarConfig.status.value, allowPartialConfiguration = true)
                }
            }
        }
    }

    @CallSuper
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        if (!enable) {
            return
        }
        networkReceiver = NetworkConnectedReceiver(this)

        loginListener = createLoginListener()
        authServiceConnection = serviceConnection(radarApp.authService)
        config = radarConfig
        configReceiver = false
        handler = Handler(Looper.getMainLooper())
        startedAt = SystemClock.elapsedRealtime()

        createView()

        lifecycleScope.launch {
            launch {
                repeatOnLifecycle(Lifecycle.State.STARTED) {
                    networkReceiver.monitor()
                }
            }
            configCollectorJob = launch {
                repeatOnLifecycle(Lifecycle.State.STARTED) {
                    config.config.
                    collect {
                        updateConfig(config.status.value, allowPartialConfiguration = false)
                    }
                }
            }

            launch {
                authServiceConnection.state
                    .collect { bindState: BindState<AuthService.AuthServiceBinder> ->
                        when (bindState) {
                            is ManagedServiceConnection.BoundService -> {
                                bindState.binder.also {
                                    authSplashBinder = it
                                    splashServiceBoundActions.launchJoin { action ->
                                        action(it)
                                    }
                                }
                            }

                            is ManagedServiceConnection.Unbound -> {
                                authSplashBinder?.let { binder ->
                                    splashUnboundActions.launchJoin { action ->
                                        action(binder)
                                    }
                                }
                                authSplashBinder = null
                            }
                        }
                    }
            }
        }
    }

    protected abstract fun createView()
    protected abstract fun updateView()

    override fun onStart() {
        super.onStart()
        if (!enable) {
            return
        }

        logger.info("Starting SplashActivity.")


        lifecycleScope.launch(Dispatchers.Default) {
            when (config.status.value) {
                RadarConfiguration.RemoteConfigStatus.UNAVAILABLE -> {
                    logger.info("Firebase unavailable")
                    updateState(STATE_FIREBASE_UNAVAILABLE)
                }

                RadarConfiguration.RemoteConfigStatus.FETCHED -> {
                    logger.info("Firebase fetched, starting AuthService")
                    startAuthConnection()
                }

                else -> {
                    logger.info("Starting listening for configuration updates")
                    if (networkReceiver.latestState !is NetworkConnectedReceiver.NetworkState.Connected) {
                        updateState(STATE_DISCONNECTED)
                    }
                    startConfigReceiver()
                }
            }
        }
    }

    override fun onStop() {
        super.onStop()
        if (!enable) {
            return
        }
        logger.info("Stopping splash")
        stopConfigListener()
        stopAuthConnection()
    }

    protected open fun updateState(newState: Int) {
        if (newState != state) {
            state = newState

            lifecycleScope.launch(Dispatchers.Main.immediate) {
                updateView()
            }
        }
    }

    protected open fun createLoginListener(): LoginListener {
        return object : LoginListener {
            override fun loginFailed(manager: LoginManager?, ex: Exception?) {
                if (ex != null && ex is IOException && ex !is AuthenticationException) {
                    updateState(STATE_DISCONNECTED)
                } else {
                    startActivity(radarApp.loginActivity)
                }
            }

            override fun logoutSucceeded(manager: LoginManager?, authState: AppAuthState) = Unit

            override fun loginSucceeded(manager: LoginManager?, authState: AppAuthState) {
                if (authState.isPrivacyPolicyAccepted) {
                    startActivity(radarApp.mainActivity)
                } else {
                    startActivity(radarApp.loginActivity)
                }
            }
        }
    }

    protected open fun startActivity(activity: Class<out Activity>) {
        logger.debug("Scheduling start of SplashActivity")
        lifecycleScope.launch {
            if (state == STATE_FINISHED) {
                return@launch
            }
            updateState(STATE_STARTING)
            startActivityFuture?.cancel()
            Runnable {
                updateState(STATE_FINISHED)

                logger.info("Starting SplashActivity")
                Intent(this@SplashActivity, activity).also {
                    it.flags = FLAG_ACTIVITY_NEW_TASK or FLAG_ACTIVITY_TASK_ON_HOME or FLAG_ACTIVITY_CLEAR_TOP or FLAG_ACTIVITY_SINGLE_TOP
                    onWillStartActivity()
                    startActivity(it)
                    onDidStartActivity()
                }
                finish()
            }.also { runnable ->
//                startActivityFuture = runnable
                val delayRemaining = delayMs - (SystemClock.elapsedRealtime() - startedAt)
                startActivityFuture = lifecycleScope.launch {
                    delay(delayRemaining.coerceAtLeast(0))
                    runnable.run()
                }
            }
        }
    }

    protected open fun onWillStartActivity() {}

    protected open fun onDidStartActivity() {}

    protected open suspend fun startConfigReceiver() {
        updateState(STATE_FETCHING_CONFIG)
        config.forceFetch()
        if (configCollectorJob != null) {
            configReceiver = true
        }
    }

    protected open suspend fun startAuthConnection() {
        if (authServiceConnection.state.value !is ManagedServiceConnection.BoundService) {
            updateState(STATE_AUTHORIZING)
            authServiceConnection.bind()
        }
    }

    protected open fun stopConfigListener() {
        if (configReceiver) {
            configCollectorJob?.cancel()
            configCollectorJob = null
            configReceiver = false
        }
    }

    protected open fun stopAuthConnection() {
        authServiceConnection.unbind()
    }

    @Keep
    companion object {
        private val logger = LoggerFactory.getLogger(SplashActivity::class.java)

        const val STATE_INITIAL = 1
        const val STATE_FETCHING_CONFIG = 2
        const val STATE_AUTHORIZING = 3
        const val STATE_STARTING = 4
        const val STATE_DISCONNECTED = 5
        const val STATE_FIREBASE_UNAVAILABLE = 6
        const val STATE_FINISHED = 7
    }
}
