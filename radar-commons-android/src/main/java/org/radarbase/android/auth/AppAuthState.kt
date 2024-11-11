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

package org.radarbase.android.auth

import android.os.SystemClock
import androidx.annotation.Keep
import io.ktor.http.*
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import org.radarbase.android.auth.LoginManager.Companion.AUTH_TYPE_UNKNOWN
import org.radarbase.android.auth.portal.ManagementPortalClient.Companion.SOURCE_IDS_PROPERTY
import org.radarbase.android.util.buildJsonArray
import org.radarbase.android.util.equalTo
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.TimeUnit

/** Authentication state of the application.  */
@Keep
@Suppress("unused")
class AppAuthState(builder: Builder) {
    val lastUpdate: Long = builder.lastUpdate

    val projectId: String? = builder.projectId
    val userId: String? = builder.userId
    val token: String? = builder.token
    val tokenType: Int = builder.tokenType
    val authenticationSource: String? = builder.authenticationSource
    val needsRegisteredSources: Boolean = builder.needsRegisteredSources
    val expiration: Long = builder.expiration
    val attributes: Map<String, String> = HashMap(builder.attributes)
    val headers: List<Pair<String, String>> = ArrayList(builder.headers)
    val sourceMetadata: List<SourceMetadata>
    val sourceTypes: List<SourceType>
    val isPrivacyPolicyAccepted: Boolean = builder.isPrivacyPolicyAccepted
    val ktorHeaders = Headers.build {
        headers.forEach { (k, v) -> append(k, v) }
    }
    val baseUrl: String? = attributes[AuthService.BASE_URL_PROPERTY]?.trimEndSlash()

    val isValid: Boolean
        get() = isPrivacyPolicyAccepted && expiration > System.currentTimeMillis()

    val isInvalidated: Boolean
        get() = expiration == 0L

    constructor() : this(Builder())

    constructor(initializer: Builder.() -> Unit) : this(Builder().also(initializer))

    init {
        sourceTypes = buildList {
            addAll(builder.sourceTypes)
            sourceMetadata = builder.sourceMetadata.map { it.deduplicateType(this) }
        }
    }

    fun getAttribute(key: String) = attributes[key]

    fun serializableAttributeList() = buildJsonArray {
        attributes.forEach { (k, v) ->
            put(k)
            put(v)
        }
    }.toString()

    fun serializableHeaderList() = buildJsonArray {
        headers.forEach { (k, v) ->
            put(k)
            put(v)
        }
    }.toString()

    fun isValidFor(time: Long, unit: TimeUnit) = isPrivacyPolicyAccepted
            && expiration - unit.toMillis(time) > System.currentTimeMillis()

    fun isAuthorizedForSource(sourceId: String?): Boolean {
        if (!needsRegisteredSources) return true
        sourceId ?: return false
        val registeredSourceIds = attributes[SOURCE_IDS_PROPERTY] ?: return false
        return sourceId in registeredSourceIds
    }

    val timeSinceLastUpdate: Long
        get() = SystemClock.elapsedRealtime() - lastUpdate

    fun reset(): AppAuthState {
        return Builder().build()
    }

    suspend fun alter(changes: suspend Builder.() -> Unit): AppAuthState {
        return Builder(this).also {
            it.projectId = projectId
            it.userId = userId
            it.token = token
            it.tokenType = tokenType
            it.expiration = expiration
            it.authenticationSource = authenticationSource
            it.isPrivacyPolicyAccepted = isPrivacyPolicyAccepted
            it.needsRegisteredSources = needsRegisteredSources

            it.attributes += attributes
            it.sourceMetadata += sourceMetadata
            it.sourceTypes += sourceTypes
            it.headers += headers
            it.changes()
        }.build()
    }

    class Builder(val original: AppAuthState? = null) {
        val lastUpdate = SystemClock.elapsedRealtime()

        val headers: MutableCollection<Pair<String, String>> = mutableListOf()
        val sourceMetadata: MutableCollection<SourceMetadata> = mutableListOf()
        val attributes: MutableMap<String, String> = mutableMapOf()

        var needsRegisteredSources = true

        var projectId: String? = null
        var userId: String? = null
        var token: String? = null
        var authenticationSource: String? = null
        var tokenType = AUTH_TYPE_UNKNOWN
        var expiration: Long = 0
        var isPrivacyPolicyAccepted = false
        val sourceTypes: MutableCollection<SourceType> = mutableListOf()

        fun clear() {
            headers.clear()
            sourceMetadata.clear()
            sourceTypes.clear()
            attributes.clear()

            needsRegisteredSources = true
            projectId = null
            userId = null
            token = null
            tokenType = AUTH_TYPE_UNKNOWN
            authenticationSource = null
            expiration = 0
            isPrivacyPolicyAccepted = false
        }

        fun parseAttributes(jsonString: String?) {
            jsonString ?: return
            Json.decodeFromString<Sequence<String>>(jsonString)
                .zipWithNext()
                .filterIndexed { i, _ -> i % 2 == 0 }
                .toMap(attributes)
        }

        fun invalidate() {
            expiration = 0L
        }

        fun setHeader(name: String, value: String) {
            headers.removeAll { (k, _) -> k == name }
            addHeader(name, value)
        }

        fun addHeader(name: String, value: String) {
            headers += name to value
        }

        fun parseHeaders(jsonString: String?) {
            jsonString ?: return
            Json.decodeFromString<Sequence<String>>(jsonString)
                .zipWithNext()
                .filterIndexedTo(this.headers) { i, _ -> i % 2 == 0 }
        }

        fun parseSourceTypes(sourceJson: Collection<String>?) {
            sourceJson ?: return
            sourceJson.mapTo(sourceTypes) { Json.decodeFromString(it) }
        }

        fun parseSourceMetadata(sourceJson: Collection<String>?) {
            sourceJson ?: return
            sourceJson.mapTo(sourceMetadata) { Json.decodeFromString(it) }
        }

        fun build(): AppAuthState = AppAuthState(this)
    }

    override fun toString(): String {
        return """
            AppAuthState{
                authenticationSource='$authenticationSource',
                projectId='$projectId',
                userId='$userId',
                token='$token',
                tokenType=$tokenType,
                expiration=$expiration,
                lastUpdate=$lastUpdate,
                attributes=$attributes,
                sourceTypes=$sourceTypes,
                sourceMetadata=$sourceMetadata,
                parseHeaders=$headers,
                isPrivacyPolicyAccepted=$isPrivacyPolicyAccepted,
            }
        """.trimIndent()
    }

    override fun equals(other: Any?) = equalTo(
        other,
        AppAuthState::lastUpdate,
        AppAuthState::projectId,
        AppAuthState::userId,
        AppAuthState::token,
        AppAuthState::tokenType,
        AppAuthState::authenticationSource,
        AppAuthState::needsRegisteredSources,
        AppAuthState::expiration,
        AppAuthState::attributes,
        AppAuthState::headers,
        AppAuthState::sourceMetadata,
        AppAuthState::sourceTypes,
        AppAuthState::isPrivacyPolicyAccepted,
    )

    override fun hashCode(): Int = Objects.hash(projectId, userId, token)

    companion object {
        private val logger = LoggerFactory.getLogger(AppAuthState::class.java)

        /**
         * Strips all slashes from the end of a URL.
         * @receiver string to strip
         * @return stripped URL or null if that would result in an empty or null string.
         */
        private fun String.trimEndSlash(): String? {
            val result = trimEnd('/')
            if (result.isEmpty()) {
                logger.warn("Base URL '{}' should be a valid URL.", this)
                return null
            }
            return result
        }
    }
}
