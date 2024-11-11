package org.radarbase.android.source

import org.radarbase.android.auth.AppAuthState
import org.radarbase.android.auth.AuthService
import org.radarbase.android.auth.AuthService.Companion.RETRY_MAX_DELAY
import org.radarbase.android.auth.AuthService.Companion.RETRY_MIN_DELAY
import org.radarbase.android.auth.LoginListener
import org.radarbase.android.auth.LoginManager
import org.radarbase.android.auth.SourceMetadata
import org.radarbase.android.util.CoroutineTaskExecutor
import org.radarbase.android.util.DelayedRetry
import org.slf4j.LoggerFactory
import java.io.Closeable

class SourceProviderRegistrar(
    private val authServiceBinder: AuthService.AuthServiceBinder,
    private val executor: CoroutineTaskExecutor,
    private val providers: List<SourceProvider<*>>,
    val onUpdate: (unregisteredProviders: List<SourceProvider<*>>, registeredProviders: List<SourceProvider<*>>) -> Unit
): LoginListener, Closeable {
    private val authRegistration: AuthService.LoginListenerRegistry = executor.nonSuspendingCompute {
        authServiceBinder.addLoginListener(this)
    }
    private var isClosed: Boolean = false
    private val retry: MutableMap<SourceProvider<*>, Pair<DelayedRetry, CoroutineTaskExecutor.CoroutineFutureHandle>> = mutableMapOf()

    override fun loginSucceeded(manager: LoginManager?, authState: AppAuthState) {
        executor.execute {
            resetRetry()

            val (legalProviders, illegalProviders) = providers.partition { it.canRegisterFor(authState, false) }
            val (registeredProviders, unregisteredProviders) = legalProviders.partition { it.isRegisteredFor(authState, false) }

            val unregisteredSourceMetadata = mutableListOf<SourceMetadata>()
            unregisteredProviders.forEach { provider ->
                val unregistered = authState.sourceMetadata.filter { provider.matches(it.type, false) }
                if (unregistered.isEmpty()) {
                    registerProvider(provider, authState)
                } else {
                    unregisteredSourceMetadata += unregistered
                }
            }
            if (unregisteredSourceMetadata.isNotEmpty()) {
                authServiceBinder.unregisterSources(unregisteredSourceMetadata)
            }

            onUpdate(illegalProviders + unregisteredProviders, registeredProviders)
        }
    }

    private suspend fun registerProvider(provider: SourceProvider<*>, authState: AppAuthState) {
        if (isClosed) {
            return
        }
        val sourceType = authState.sourceTypes.first { provider.matches(it, false) }
        authServiceBinder.registerSource(SourceMetadata(sourceType), { _, _ ->
            retry -= provider
        }, { ex ->
            logger.error("Failed to register source {}. Trying again", sourceType, ex)
            executor.executeReentrant {
                if (isClosed) { return@executeReentrant }
                val delay = retry[provider]?.first ?: DelayedRetry(RETRY_MIN_DELAY, RETRY_MAX_DELAY)
                val future = executor.delay(delay.nextDelay()) { registerProvider(provider, authState) }
                if (future != null) {
                    retry[provider] = Pair(delay, future)
                } else {
                    retry -= provider
                }
            }
        })
    }

    override fun loginFailed(manager: LoginManager?, ex: Exception?) {
    }

    override fun logoutSucceeded(manager: LoginManager?, authState: AppAuthState) = Unit

    private fun resetRetry() {
        retry.values.forEach { (_, future) ->
            future.cancel()
        }
        retry.clear()
    }

    override fun close() {
        executor.execute {
            authServiceBinder.removeLoginListener(authRegistration)
        }
            executor.executeReentrant {
            resetRetry()
            isClosed = true
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(SourceProviderRegistrar::class.java)
    }
}
