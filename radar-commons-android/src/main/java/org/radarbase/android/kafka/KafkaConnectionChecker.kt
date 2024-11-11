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

package org.radarbase.android.kafka

import android.os.SystemClock
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.radarbase.android.data.DataHandler
import org.radarbase.android.util.DelayedRetry
import org.radarbase.android.util.runSafeOrNull
import org.radarbase.producer.AuthenticationException
import org.radarbase.producer.KafkaSender
import org.radarbase.producer.rest.ConnectionState
import org.slf4j.LoggerFactory
import kotlin.coroutines.CoroutineContext

/**
 * Checks the connection of a sender. It does so using two mechanisms: a regular
 * heartbeat signal when the connection is assumed to be
 * present, and a exponential back-off mechanism if the connection is severed. If the connection is
 * assessed to be present through another mechanism, [didConnect] should be called,
 * conversely, if it is assessed to be severed, [didDisconnect] should be
 * called.
 */
internal class KafkaConnectionChecker(
    private val sender: KafkaSender,
    private val listener: DataHandler<*, *>,
    heartbeatSecondsInterval: Long,
    checkerCoroutineContext: CoroutineContext = Dispatchers.Default
) {
    private var future: Job? = null
    private val heartbeatInterval: Long = heartbeatSecondsInterval * 1000L
    private val retryDelay = DelayedRetry(INCREMENTAL_BACKOFF_MILLISECONDS, MAX_BACKOFF_MILLISECONDS)
    private var lastConnection: Long = -1L

    private val _isConnected = MutableStateFlow(false)
    val isConnected: StateFlow<Boolean> = _isConnected

    private val checkerExceptionHandler: CoroutineExceptionHandler =
        CoroutineExceptionHandler { _, throwable ->
            logger.error("CoroutineExceptionHandler - Exception when checking connection: ", throwable)
        }

    private val job: Job = SupervisorJob()
    private val connectionCheckScope: CoroutineScope = CoroutineScope(
        checkerCoroutineContext + job + CoroutineName("connection-checker") + checkerExceptionHandler
    )


    suspend fun initialize() {
        connectionCheckScope.launch {
            logger.runSafeOrNull {
                if (sender.connectionState.first() == ConnectionState.State.CONNECTED) {
                    _isConnected.value = false
                    didConnect()
                } else {
                    _isConnected.value = true
                    didDisconnect(null)
                }
            }
        }
    }

    /**
     * Check whether the connection was closed and try to reconnect.
     */
    private suspend fun makeCheck() {
        try {
            if (!isConnected.value) {
                if (sender.resetConnection()) {
                    didConnect()
                    listener.serverStatus.value = ServerStatus.CONNECTED
                    logger.info("Sender reconnected")
                } else {
                    future?.cancel()
                    future = null
                    retry()
                }
            } else if (SystemClock.uptimeMillis() - lastConnection > 15_000L) {
                if (sender.connectionState.first() == ConnectionState.State.CONNECTED || sender.resetConnection()) {
                    didConnect()
                } else {
                    didDisconnect(null)
                }
            }
        } catch (ex: AuthenticationException) {
            didDisconnect(ex)
        }
    }

    /** Check the connection as soon as possible.  */
    suspend fun check() {
        connectionCheckScope.launch {
            logger.runSafeOrNull {
                makeCheck()
            }
        }
    }

    /** Retry the connection with an incremental backoff.  */
    private fun retry() {
        future = connectionCheckScope.launch {
            delay(retryDelay.nextDelay())
            logger.runSafeOrNull {
                makeCheck()
            }
        }
    }

    /** Signal that the sender successfully connected.  */
    fun didConnect() {
        connectionCheckScope.launch {
            logger.runSafeOrNull {
                lastConnection = SystemClock.uptimeMillis()
                if (!_isConnected.value) {
                    _isConnected.value = true
                    future?.also {
                        it.cancel()
                        future = null
                    }

                    future = connectionCheckScope.launch {
                        while (isActive) {
                            delay(heartbeatInterval)
                            logger.runSafeOrNull {
                                makeCheck()
                            }
                        }
                    }
                }
                retryDelay.reset()
            }
        }
    }

    /**
     * Signal that the Kafka REST sender has disconnected.
     * @param ex exception the sender disconnected with, may be null
     */
    fun didDisconnect(ex: Exception?) {
        connectionCheckScope.launch {
            logger.runSafeOrNull {
                logger.warn("Sender is disconnected", ex)

                if (_isConnected.value) {
                    _isConnected.value = false
                    future?.let {
                        it.cancel()
                        future = null
                    }
                    future = connectionCheckScope.launch {
                        delay(INCREMENTAL_BACKOFF_MILLISECONDS)
                        logger.runSafeOrNull {
                            makeCheck()
                        }
                    }
                    if (ex is AuthenticationException) {
                        logger.warn("Failed to authenticate to server: {}", ex.message)
                        listener.serverStatus.value = ServerStatus.UNAUTHORIZED
                    } else {
                        listener.serverStatus.value = ServerStatus.DISCONNECTED
                    }
                }
            }
        }
    }

    fun stopHeartbeats() {
        future?.cancel()
        future = null
    }

    companion object {
        private val logger = LoggerFactory.getLogger(KafkaConnectionChecker::class.java)

        private const val INCREMENTAL_BACKOFF_MILLISECONDS = 60 * 1000L // 1 minute
        private const val MAX_BACKOFF_MILLISECONDS = 4 * 60 * 60 * 1000L // 4 hours
    }
}
