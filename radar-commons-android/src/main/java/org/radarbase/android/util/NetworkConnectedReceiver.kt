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

package org.radarbase.android.util

import android.annotation.SuppressLint
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.net.ConnectivityManager
import android.net.ConnectivityManager.CONNECTIVITY_ACTION
import android.net.ConnectivityManager.NetworkCallback
import android.net.ConnectivityManager.TYPE_ETHERNET
import android.net.ConnectivityManager.TYPE_WIFI
import android.net.Network
import android.net.NetworkCapabilities
import android.os.Build
import androidx.annotation.RequiresApi
import kotlinx.coroutines.channels.Channel.Factory.CONFLATED
import kotlinx.coroutines.channels.ProducerScope
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.callbackFlow
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Keeps track of whether there is a network connection (e.g., WiFi or Ethernet).
 */
@SuppressLint("ObsoleteSdkInt")
class NetworkConnectedReceiver(
    private val context: Context
) {
    private val connectivityManager: ConnectivityManager = requireNotNull(
        context.getSystemService(Context.CONNECTIVITY_SERVICE) as? ConnectivityManager
    ) { "No connectivity manager available" }

    private val isMonitoring: AtomicBoolean = AtomicBoolean(false)

    var state: Flow<NetworkState>? = null

    var latestState: NetworkState = NetworkState.Disconnected
        get() {
            if (state == null) {
                return NetworkState.Disconnected
            }
            return field
        }
        private set

    suspend fun monitor() {
        if (isMonitoring.get()) {
            logger.info("Network receiver is already being monitored")
            return
        }
        state = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.N) {
            callbackFlow {
                val callback = createCallback()
                connectivityManager.registerDefaultNetworkCallback(callback)
                isMonitoring.set(true)
                awaitClose {
                    connectivityManager.unregisterNetworkCallback(callback)
                    state = null
                    isMonitoring.set(false)
                }
            }
        } else {
            callbackFlow {
                val receiver = createBroadcastReceiver()
                @Suppress("DEPRECATION")
                context.registerReceiver(receiver, IntentFilter(CONNECTIVITY_ACTION))
                isMonitoring.set(true)
                awaitClose {
                    context.unregisterReceiver(receiver)
                    state = null
                    isMonitoring.set(false)
                }
            }
        }
            .buffer(CONFLATED)
    }

    @Suppress("DEPRECATION")
    private fun ProducerScope<NetworkState>.createBroadcastReceiver(): BroadcastReceiver {
        return object : BroadcastReceiver() {
            override fun onReceive(context: Context, intent: Intent?) {
                if (intent?.action == CONNECTIVITY_ACTION) {
                    trySendBlocking(
                        connectivityManager.activeNetworkInfo?.let {
                            val networkType = it.type
                            NetworkState.Connected(networkType == TYPE_WIFI || networkType == TYPE_ETHERNET)
                    } ?: NetworkState.Disconnected)

                }
            }
        }
    }

    @RequiresApi(Build.VERSION_CODES.N)
    private fun ProducerScope<NetworkState>.createCallback(): NetworkCallback {
        return object : NetworkCallback() {
            var state: NetworkState = NetworkState.Disconnected
            override fun onAvailable(network: Network) {
                transition { previousState -> NetworkState.Connected(previousState.hasWifiOrEthernet) }
            }

            override fun onCapabilitiesChanged(network: Network, capabilities: NetworkCapabilities) {
                transition { previousState ->
                    if (previousState != NetworkState.Disconnected) {
                        NetworkState.Connected(
                            capabilities.hasTransport(NetworkCapabilities.TRANSPORT_WIFI) ||
                                    capabilities.hasTransport(NetworkCapabilities.TRANSPORT_ETHERNET)
                        )
                    } else {
                        NetworkState.Disconnected
                    }
                }
            }

            override fun onUnavailable() {
                // nothing happened
            }

            override fun onLost(network: Network) {
                transition { NetworkState.Disconnected }
            }

            @Synchronized
            private fun transition(transition: (NetworkState) -> NetworkState) {
                this.state = transition(this.state)
                latestState = this.state
                trySendBlocking(this.state)
            }
        }
    }

    sealed class NetworkState(
        val hasWifiOrEthernet: Boolean
    ) {
        fun hasConnection(needsWifiOrEthernetOnly: Boolean): Boolean = when(this) {
            is Disconnected -> false
            is Connected -> !needsWifiOrEthernetOnly || hasWifiOrEthernet
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false
            other as NetworkState
            return hasWifiOrEthernet == other.hasWifiOrEthernet
        }
        override fun hashCode(): Int = hasWifiOrEthernet.hashCode()

        object Disconnected : NetworkState(hasWifiOrEthernet = false)
        class Connected(hasWifiOrEthernet: Boolean) : NetworkState(hasWifiOrEthernet)
    }

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(NetworkConnectedReceiver::class.java)
    }
}
