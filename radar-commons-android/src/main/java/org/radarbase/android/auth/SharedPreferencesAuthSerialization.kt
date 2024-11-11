package org.radarbase.android.auth

import android.annotation.SuppressLint
import android.content.Context
import android.content.SharedPreferences
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.json.JSONException
import org.radarbase.android.auth.portal.ManagementPortalClient
import org.slf4j.LoggerFactory

@SuppressLint("CommitPrefEdits")
class SharedPreferencesAuthSerialization(context: Context): AuthSerialization {
    private val prefLock = Mutex()
    private val prefs = context.getSharedPreferences(AUTH_PREFS, Context.MODE_PRIVATE)

    override suspend fun load(): AppAuthState? {
        val builder = AppAuthState.Builder()
        prefLock.withLock {
            try {
                val sourceMetadataSet = prefs.getStringSet(LOGIN_APP_SOURCES_LIST, null)
                if (sourceMetadataSet != null) builder.parseSourceMetadata(sourceMetadataSet)
            } catch (ex: JSONException) {
                logger.warn("Cannot parse source metadata parseHeaders", ex)
            }
            try {
                val sourceTypeSet = prefs.getStringSet(LOGIN_SOURCE_TYPES, null)
                if (sourceTypeSet != null) builder.parseSourceTypes(sourceTypeSet)
            } catch (ex: JSONException) {
                logger.warn("Cannot parse source types parseHeaders", ex)
            }
            builder.apply {
                projectId = prefs.getString(LOGIN_PROJECT_ID, null)
                userId = prefs.getString(LOGIN_USER_ID, null) ?: return null
                token = prefs.getString(LOGIN_TOKEN, null)
                tokenType = prefs.getInt(LOGIN_TOKEN_TYPE, 0)
                expiration = prefs.getLong(LOGIN_EXPIRATION, 0L)
                parseAttributes(prefs.getString(LOGIN_ATTRIBUTES, null))
                parseHeaders(prefs.getString(LOGIN_HEADERS_LIST, null))
                isPrivacyPolicyAccepted = prefs.getBoolean(LOGIN_PRIVACY_POLICY_ACCEPTED, false)
                needsRegisteredSources = prefs.getBoolean(LOGIN_NEEDS_REGISTERD_SOURCES, true)
            }
        }
        return builder.build()
    }

    override suspend fun store(state: AppAuthState) {
        val editor = prefs.edit().apply {
            putString(LOGIN_PROJECT_ID, state.projectId)
            putString(LOGIN_USER_ID, state.userId)
            putString(LOGIN_TOKEN, state.token)
            putString(LOGIN_HEADERS_LIST, state.serializableHeaderList())
            putString(LOGIN_ATTRIBUTES, state.serializableAttributeList())
            putInt(LOGIN_TOKEN_TYPE, state.tokenType)
            putLong(LOGIN_EXPIRATION, state.expiration)
            putBoolean(LOGIN_PRIVACY_POLICY_ACCEPTED, state.isPrivacyPolicyAccepted)
            putString(LOGIN_AUTHENTICATION_SOURCE, state.authenticationSource)
            putBoolean(LOGIN_NEEDS_REGISTERD_SOURCES, state.needsRegisteredSources)
            putStringSet(LOGIN_APP_SOURCES_LIST, state.sourceMetadata.mapTo(HashSet()) {
                Json.encodeToString(it)
            })
            putStringSet(LOGIN_SOURCE_TYPES, state.sourceTypes.mapTo(HashSet()) {
                Json.encodeToString(it)
            })
            remove(ManagementPortalClient.SOURCES_PROPERTY)
        }
        editor.lockedCommit()
    }

    override suspend fun remove() {
        val editor = prefs.edit().apply {
            remove(LOGIN_PROJECT_ID)
            remove(LOGIN_USER_ID)
            remove(LOGIN_TOKEN)
            remove(LOGIN_HEADERS_LIST)
            remove(LOGIN_ATTRIBUTES)
            remove(LOGIN_TOKEN_TYPE)
            remove(LOGIN_EXPIRATION)
            remove(LOGIN_PRIVACY_POLICY_ACCEPTED)
            remove(LOGIN_AUTHENTICATION_SOURCE)
            remove(LOGIN_NEEDS_REGISTERD_SOURCES)
            remove(LOGIN_APP_SOURCES_LIST)
            remove(LOGIN_SOURCE_TYPES)
            remove(ManagementPortalClient.SOURCES_PROPERTY)
        }
        editor.lockedCommit()
    }

    private suspend fun SharedPreferences.Editor.lockedCommit() = prefLock.withLock {
        withContext(Dispatchers.IO) {
            commit()
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(SharedPreferencesAuthSerialization::class.java)

        private const val LOGIN_PROJECT_ID = "org.radarcns.android.auth.AppAuthState.projectId"
        private const val LOGIN_USER_ID = "org.radarcns.android.auth.AppAuthState.userId"
        private const val LOGIN_TOKEN = "org.radarcns.android.auth.AppAuthState.token"
        private const val LOGIN_TOKEN_TYPE = "org.radarcns.android.auth.AppAuthState.tokenType"
        private const val LOGIN_EXPIRATION = "org.radarcns.android.auth.AppAuthState.expiration"
        internal const val AUTH_PREFS = "org.radarcns.auth"
        private const val LOGIN_ATTRIBUTES = "org.radarcns.android.auth.AppAuthState.attributes"
        private const val LOGIN_HEADERS_LIST = "org.radarcns.android.auth.AppAuthState.headerList"
        private const val LOGIN_APP_SOURCES_LIST = "org.radarcns.android.auth.AppAuthState.appSourcesList"
        private const val LOGIN_PRIVACY_POLICY_ACCEPTED = "org.radarcns.android.auth.AppAuthState.isPrivacyPolicyAccepted"
        private const val LOGIN_AUTHENTICATION_SOURCE = "org.radarcns.android.auth.AppAuthState.authenticationSource"
        private const val LOGIN_NEEDS_REGISTERD_SOURCES = "org.radarcns.android.auth.AppAuthState.needsRegisteredSources"
        private const val LOGIN_SOURCE_TYPES = "org.radarcns.android.auth.AppAuthState.sourceTypes"
    }
}
