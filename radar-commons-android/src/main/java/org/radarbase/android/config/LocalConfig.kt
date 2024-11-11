package org.radarbase.android.config

import java.util.concurrent.ConcurrentMap

interface LocalConfig {
    val hasChanges: Boolean
    val config: ConcurrentMap<String, String>

    /**
     * Adds a new or updated setting to the local configuration. This will be persisted to
     * SharedPreferences. Using this will override Firebase settings. Setting it to `null`
     * means that the default value in code will be used, not the Firebase setting. Use
     * [.reset] to completely unset any local configuration.
     *
     * @param key configuration name
     * @param value configuration value
     * @return previous local value for given name, if any
     */
    fun put(key: String, value: Any): String?
    fun removeAll(keys: Array<out String>)
    operator fun minusAssign(keys: Array<out String>) = removeAll(keys)
    suspend fun persistChanges(): Boolean
    val keys: Set<String>
    operator fun get(key: String): String?
    operator fun contains(key: String): Boolean
    fun toMap(): Map<String, String>
}
