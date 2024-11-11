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

package org.radarbase.android.source

import android.content.Intent
import android.os.Bundle
import androidx.annotation.CallSuper
import androidx.annotation.Keep
import androidx.lifecycle.LifecycleService
import androidx.lifecycle.MutableLiveData
import androidx.lifecycle.lifecycleScope
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import org.apache.avro.specific.SpecificRecord
import org.radarbase.android.RadarApplication.Companion.radarApp
import org.radarbase.android.RadarApplication.Companion.radarConfig
import org.radarbase.android.RadarConfiguration
import org.radarbase.android.auth.*
import org.radarbase.android.config.SingleRadarConfiguration
import org.radarbase.android.data.DataHandler
import org.radarbase.android.source.SourceProvider.Companion.MODEL_KEY
import org.radarbase.android.source.SourceProvider.Companion.NEEDS_BLUETOOTH_KEY
import org.radarbase.android.source.SourceProvider.Companion.PLUGIN_NAME_KEY
import org.radarbase.android.source.SourceProvider.Companion.PRODUCER_KEY
import org.radarbase.android.util.BluetoothStateReceiver.Companion.bluetoothIsEnabled
import org.radarbase.android.util.BundleSerialization
import org.radarbase.android.util.CoroutineTaskExecutor
import org.radarbase.android.util.ManagedServiceConnection
import org.radarbase.android.util.ManagedServiceConnection.Companion.serviceConnection
import org.radarbase.kotlin.coroutines.launchJoin
import org.radarcns.kafka.ObservationKey
import org.slf4j.LoggerFactory
import java.io.IOException
import java.text.SimpleDateFormat
import java.util.*

    /**
 * A service that manages a SourceManager and a TableDataHandler to send addToPreferences the data of a
 * wearable device, phone or API and send it to a Kafka REST proxy.
 *
 * Specific wearables should extend this class.
 */
@Keep
abstract class SourceService<T : BaseSourceState> :
    LifecycleService(), SourceStatusListener, LoginListener
{
    private var registrationFuture: CoroutineTaskExecutor.CoroutineFutureHandle? = null
    val key = ObservationKey()

    @get:Synchronized
    @set:Synchronized
    var dataHandler: DataHandler<ObservationKey, SpecificRecord>? = null
    @get:Synchronized
    var sourceManager: SourceManager<T>? = null
        private set
    private lateinit var mBinder: SourceServiceBinder<T>
    private var hasBluetoothPermission: Boolean = false
    var sources: List<SourceMetadata> = emptyList()
    var sourceTypes: Set<SourceType> = emptySet()
    var sourceConnectFailed: MutableSharedFlow<SourceConnectFailed> = MutableSharedFlow()

    val status: MutableStateFlow<SourceStatusListener.Status> =
        MutableStateFlow(SourceStatusListener.Status.DISCONNECTED)


    private val acceptableSourceTypes: Set<SourceType>
        get() = sourceTypes.filterTo(HashSet()) {
            it.producer.equals(sourceProducer, ignoreCase = true)
                    && it.model.equals(sourceModel, ignoreCase = true)
        }

    private val acceptableSources: List<SourceMetadata>
        get() = sources.filter { it.type in acceptableSourceTypes }

    private val isAuthorizedForSource: Boolean
        get() = !needsRegisteredSources || acceptableSources.isNotEmpty()

    private lateinit var authConnection: ManagedServiceConnection<AuthService.AuthServiceBinder>
    private var authListenerRegistry: AuthService.LoginListenerRegistry? = null
    private val authBoundTasks: MutableList<suspend (AuthService.AuthServiceBinder) -> Unit> = mutableListOf(
        { binder ->
            authListenerRegistry = binder.addLoginListener(this)
            binder.refreshIfOnline()
        }
    )

    private val authUnboundTasks: MutableList<AuthServiceStateReactor> = mutableListOf(
        { binder ->
            authListenerRegistry?.let {
                binder.removeLoginListener(it)
            }
        }
    )
    
    protected lateinit var config: RadarConfiguration
    private lateinit var radarConnection: ManagedServiceConnection<org.radarbase.android.IRadarBinder>
    private lateinit var sourceServiceExecutor: CoroutineTaskExecutor
    private var startFuture: CoroutineTaskExecutor.CoroutineFutureHandle? = null
    private var needsRegisteredSources: Boolean = true
    private lateinit var pluginName: String
    private lateinit var sourceModel: String
    private lateinit var sourceProducer: String
    private val name = javaClass.simpleName
    private var delayedStart: Set<String>? = null

    private var _registeredSource: SourceMetadata? = null

    private var authConnectionBinder: AuthService.AuthServiceBinder? = null

    var registeredSource: SourceMetadata?
        get() = _registeredSource
        private set(value) {
            sourceServiceExecutor.execute { _registeredSource = value }
        }

    var manualAttributes: Map<String, String> = emptyMap()
        set(value) {
            if (value == field) return
            val oldValue = field
            field = value
            // if the new manual attributes would change something about the old manual
            // attributes and a source is already assigned, then update the registration
            sourceServiceExecutor.executeReentrant {
                val currentSource = registeredSource ?: return@executeReentrant
                val bareAttributes = if (oldValue.isEmpty()) {
                    currentSource.attributes
                } else buildMap {
                    putAll(currentSource.attributes)
                    oldValue.keys.forEach { remove(it) }
                }

                if (bareAttributes + value != currentSource.attributes) {
                    val currentType = currentSource.type ?: return@executeReentrant
                    registerSource(currentSource, currentType, bareAttributes)
                }
            }

            onManualAttributesUpdate(value)
        }

    val state: T
        get() = sourceManager?.state ?: defaultState.apply {
            id.apply {
                projectId = key.projectId
                userId = key.userId
                sourceId = key.sourceId
            }
        }

    /**
     * Default state when no source manager is active.
     */
    protected abstract val defaultState: T

    private val expectedSourceNames: Set<String>
        get() = HashSet(acceptableSources.mapNotNull { it.expectedSourceName })

    open val isBluetoothConnectionRequired: Boolean
        get() = hasBluetoothPermission

    @CallSuper
    override fun onCreate() {
        logger.info("Creating SourceService {}", this)
        super.onCreate()
        sources = emptyList()
        sourceTypes = emptySet()

        mBinder = createBinder()

        authConnection = serviceConnection(radarApp.authService)
        radarConnection = serviceConnection(radarApp.radarService)

        lifecycleScope.launch {
            launch {
                radarConnection.state
                    .collect { bindState ->
                        if (bindState !is ManagedServiceConnection.BoundService) return@collect

                        dataHandler = bindState.binder.dataHandler
                        startFuture?.runNow()
                    }
            }
            launch {
                authConnection.state
                    .onEach { bindState ->
                        when (bindState) {
                            is ManagedServiceConnection.BoundService -> {
                                authConnectionBinder = bindState.binder.also { binder ->
                                    authBoundTasks.launchJoin {
                                        it(binder)
                                    }
                                }
                            }
                            is ManagedServiceConnection.Unbound -> {
                                authConnectionBinder?.also { binder ->
                                    authUnboundTasks.launchJoin {
                                        it(binder)
                                    }
                                }
                                authConnectionBinder = null
                            }
                        }
                    }.launchIn(this)
            }
            launch {
                radarConfig.config
                    .collectLatest {
                        configure(it)
                    }
            }
            radarConnection.bind()
        }
        sourceServiceExecutor = CoroutineTaskExecutor(this::class.simpleName!!)

        config = radarConfig

        sourceManager = null
        startFuture = null
    }

    @CallSuper
    override fun onDestroy() {
        logger.info("Destroying SourceService {}", this)
        super.onDestroy()

        radarConnection.unbind()
        authConnection.unbind()

        stopRecording()

        radarApp.onSourceServiceDestroy(this)
    }

    @CallSuper
    protected open fun configure(config: SingleRadarConfiguration) {
        sourceManager?.let { configureSourceManager(it, config) }
    }

    protected open fun configureSourceManager(manager: SourceManager<T>, config: SingleRadarConfiguration) {}

    override fun onBind(intent: Intent): SourceServiceBinder<T> {
        super.onBind(intent)
        doBind(intent, true)
        return mBinder
    }

    @CallSuper
    override fun onRebind(intent: Intent) {
        doBind(intent, false)
    }

    private fun doBind(intent: Intent, firstBind: Boolean) {
        logger.debug("Received (re)bind in {}", this)
        val extras = BundleSerialization.getPersistentExtras(intent, this) ?: Bundle()
        onInvocation(extras)

        radarApp.onSourceServiceInvocation(this, extras, firstBind)
    }

    override fun onUnbind(intent: Intent): Boolean {
        logger.debug("Received unbind in {}", this)
        return true
    }

    override fun sourceFailedToConnect(name: String) {
        sourceConnectFailed.tryEmit(
            SourceConnectFailed(
            this@SourceService.javaClass,
            name,
        ))
    }

    private fun broadcastSourceStatus(status: SourceStatusListener.Status) {
        this.status.value = status
    }

    override fun sourceStatusUpdated(manager: SourceManager<*>, status: SourceStatusListener.Status) {
        if (status == SourceStatusListener.Status.DISCONNECTING) {
            sourceServiceExecutor.execute {
                if (this.sourceManager === manager) {
                    this.sourceManager = null
                }
                stopSourceManager(manager)
                registeredSource = null
            }
        }
        broadcastSourceStatus(status)
    }

    @Synchronized
    private fun unsetSourceManager(): SourceManager<*>? {
        val oldSourceManager = sourceManager
        sourceManager = null
        return oldSourceManager
    }

    private fun stopSourceManager(manager: SourceManager<*>?) {
        try {
            manager?.takeUnless(SourceManager<*>::isClosed)?.close()
        } catch (e: IOException) {
            logger.warn("Failed to close source manager", e)
        }
    }

    /**
     * New source manager.
     */
    protected abstract fun createSourceManager(): SourceManager<T>

    /**
     * Handle attribute updates from the user.
     * Override to respond to changes in manual attributes.
     */
    protected open fun onManualAttributesUpdate(value: Map<String, String>) {}

    fun startRecording(acceptableIds: Set<String>) {
        if (!sourceServiceExecutor.isStarted.get()) sourceServiceExecutor.start()
        sourceServiceExecutor.execute {
            if (key.userId == null) {
                logger.error("Cannot start recording with service {}: user ID is not set.", this)
                delayedStart = acceptableIds
            } else {
                doStart(acceptableIds)
            }
        }
    }

    private fun doStart(acceptableIds: Set<String>) {
        val expectedNames = expectedSourceNames
        val actualIds = expectedNames.ifEmpty { acceptableIds }

        when {
            sourceManager?.state?.status == SourceStatusListener.Status.DISCONNECTED -> {
                logger.warn("A disconnected SourceManager is still registered for {}. Retrying later.", name)
                startAfterDelay(acceptableIds)
            }
            !isAuthorizedForSource -> {
                logger.warn("Sources have not been registered {}. Retrying later.", name)
                startAfterDelay(acceptableIds)
            }
            sourceManager != null ->
                logger.warn("A SourceManager is already registered for {}", name)
            isBluetoothConnectionRequired && !bluetoothIsEnabled ->
                logger.error("Cannot start recording for {} without Bluetooth", name)
            dataHandler != null -> {
                logger.info("Starting recording now for {}", name)
                val manager = createSourceManager()
                sourceManager = manager
                configureSourceManager(manager, radarConfig.latestConfig)
                if (state.status == SourceStatusListener.Status.UNAVAILABLE) {
                    logger.info("Status is unavailable. Not starting manager yet.")
                } else {
                    manager.start(actualIds)
                }
            }
            else -> {
                logger.info("DataHandler is not ready. Retrying later")
                startAfterDelay(acceptableIds)
            }
        }
    }

    private fun startAfterDelay(acceptableIds: Set<String>) = sourceServiceExecutor.executeReentrant {
        if (startFuture == null) {
            logger.warn("Starting recording soon for {}", name)
            startFuture = sourceServiceExecutor.delay(100) {
                startFuture?.let {
                    startFuture = null
                    doStart(acceptableIds)
                }
            }
        }
    }

    fun restartRecording(acceptableIds: Set<String>) = sourceServiceExecutor.execute {
        doStop()
        doStart(acceptableIds)
    }

    fun stopRecording() = sourceServiceExecutor.stop { doStop() }

    private fun doStop() {
        delayedStart = null
        startFuture?.let {
            it.cancel()
            startFuture = null
        }
        registrationFuture?.let {
            it.cancel()
            registrationFuture = null
        }
        stopSourceManager(unsetSourceManager())
        logger.info("Stopped recording {}", this)
    }

    override fun loginSucceeded(manager: LoginManager?, authState: AppAuthState) {
        if (!sourceServiceExecutor.isStarted.get()) {
            sourceServiceExecutor.start()
        }
        sourceServiceExecutor.execute {
            key.projectId = authState.projectId
            key.userId = authState.userId
            needsRegisteredSources = authState.needsRegisteredSources
            sourceTypes = authState.sourceTypes.toHashSet()
            sources = authState.sourceMetadata
            delayedStart?.let {
                delayedStart = null
                doStart(acceptableIds = it)
            }
        }
    }

    override fun loginFailed(manager: LoginManager?, ex: Exception?) {

    }

    override fun logoutSucceeded(manager: LoginManager?, authState: AppAuthState) = Unit

    /**
     * Override this function to get any parameters from the given intent.
     * Bundle classloader needs to be set correctly for this to work.
     *
     * @param bundle intent extras that the activity provided.
     */
    @CallSuper
    fun onInvocation(bundle: Bundle) {
        hasBluetoothPermission = bundle.getBoolean(NEEDS_BLUETOOTH_KEY, false)
        pluginName = requireNotNull(bundle.getString(PLUGIN_NAME_KEY)) { "Missing source producer" }
        sourceProducer = requireNotNull(bundle.getString(PRODUCER_KEY)) { "Missing source producer" }
        sourceModel = requireNotNull(bundle.getString(MODEL_KEY)) { "Missing source model" }
        lifecycleScope.launch {
            if (authConnection.state.value !is ManagedServiceConnection.BoundService) {
                authConnection.bind()
            }
        }
    }

    /** Get the service local binder.  */
    private fun createBinder() = SourceServiceBinder(this)

    private fun registerSource(existingSource: SourceMetadata, type: SourceType, attributes: Map<String, String>) {
        logger.info("Registering source {} with attributes {}", type, attributes)

        val source = SourceMetadata(type).apply {
            sourceId = existingSource.sourceId
            sourceName = existingSource.sourceName
            this.attributes = attributes + manualAttributes
        }

        val onFail: (Exception?) -> Unit = {
            logger.warn("Failed to register source: {}", it.toString())
            if (registrationFuture == null) {
                registrationFuture = sourceServiceExecutor.delay(300_000L) {
                    registrationFuture ?: return@delay
                    registrationFuture = null
                    registerSource(existingSource, type, attributes)
                }
            }
        }

        authConnectionBinder?.updateSource(
            source,
            { authState, updatedSource ->
                key.projectId = authState.projectId
                key.userId = authState.userId
                key.sourceId = updatedSource.sourceId
                source.sourceId = updatedSource.sourceId
                source.sourceName = updatedSource.sourceName
                source.expectedSourceName = updatedSource.expectedSourceName
                registeredSource = source
            },
            onFail,
        ) ?: onFail(null)
    }

    open fun ensureRegistration(id: String?, name: String?, attributes: Map<String, String>, onMapping: (SourceMetadata?) -> Unit) {
        sourceServiceExecutor.executeReentrant {
            if (!needsRegisteredSources) {
                onMapping(SourceMetadata(SourceType(0, sourceProducer, sourceModel, "UNKNOWN", true)).apply {
                    sourceId = id
                    sourceName = name
                    this.attributes = attributes
                })
                return@executeReentrant
            }
            if (!isAuthorizedForSource) {
                logger.warn("Cannot register source {} yet: allowed source types are empty", id)
                registrationFuture = sourceServiceExecutor.delay(100L) {
                    registrationFuture ?: return@delay
                    registrationFuture = null
                    ensureRegistration(id, name, attributes, onMapping)
                }
            }

            val matchingSource = findMatchingSource(id, name)

            if (matchingSource == null) {
                logger.warn("Cannot find matching source type for producer {} and model {}", sourceProducer, sourceModel)
            } else {
                key.sourceId = matchingSource.sourceId
                val registeredAttributes = buildMap(attributes.size + 2) {
                    putAll(attributes)
                    put("physicalId", (id ?: ""))
                    if (pluginName !in attributes) {
                        val dateFormat = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ", Locale.ROOT)
                        put(pluginName, dateFormat.format(Date()))
                    }
                }
                if (registeredAttributes.any { (k, v) -> matchingSource.attributes[k] != v }) {
                    registerSource(matchingSource, matchingSource.type!!, registeredAttributes)
                }
            }
            onMapping(matchingSource)
        }
    }

    open fun findMatchingSource(id: String?, name: String?): SourceMetadata? = acceptableSources
        .find { source ->
            val physicalId = source.attributes["physicalId"]?.takeIf { it.isNotEmpty() }
            val pluginMatches = pluginName in source.attributes
            val physicalName = source.attributes["physicalName"]?.takeIf { it.isNotEmpty() }
            when {
                pluginMatches -> true
                source.matches(id, name) -> true
                id != null && physicalId != null && id in physicalId -> true
                id != null && physicalId != null -> {
                    logger.warn("Physical id {} does not match registered id {}", physicalId, id)
                    false
                }
                physicalName != null && name != null && name in physicalName -> true
                physicalName != null -> {
                    logger.warn("Physical name {} does not match registered name {}", physicalName, name)
                    false
                }
                else -> false
            }
        }

    override fun toString() = "$name<${sourceManager?.name}>"

    companion object {
        private const val PREFIX = "org.radarcns.android."
        const val SERVER_STATUS_CHANGED = PREFIX + "ServerStatusListener.Status"
        const val SERVER_RECORDS_SENT_TOPIC = PREFIX + "ServerStatusListener.topic"
        const val SERVER_RECORDS_SENT_NUMBER = PREFIX + "ServerStatusListener.lastNumberOfRecordsSent"
        const val CACHE_TOPIC = PREFIX + "DataCache.topic"
        const val CACHE_RECORDS_UNSENT_NUMBER = PREFIX + "DataCache.numberOfRecords.first"
        const val SOURCE_STATUS_NAME = PREFIX + "SourceManager.getName"
        const val SOURCE_CONNECT_FAILED = PREFIX + "SourceStatusListener.sourceFailedToConnect"

        private val logger = LoggerFactory.getLogger(SourceService::class.java)
    }

    data class SourceConnectFailed(
        val serviceClass: Class<in SourceService<*>>,
        val sourceName: String,
    )
}
