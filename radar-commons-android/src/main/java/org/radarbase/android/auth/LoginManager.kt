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

import android.app.Activity
import androidx.annotation.Keep
import org.radarbase.producer.AuthenticationException

/** Manage a single login method.  */
@Keep
interface LoginManager {
    /**
     * Types of authentication sources that the current login manager can handle.
     * @return non-empty list of source types.
     */
    val sourceTypes: Set<String>

    fun appliesTo(authState: AppAuthState): Boolean =
        authState.authenticationSource == null || authState.authenticationSource in sourceTypes

    /**
     * With or without user interaction, refresh the current authentication state.
     * The result will be passed to a LoginListener.
     * Returns false if no refresh information is available.
     */
    suspend fun refresh(authState: AppAuthState.Builder): Boolean

    /**
     * Without user interaction, assess whether the current authentication state was valid under the
     * current login manager. This allows the app to continue if no internet connection is currently
     * available.
     * @return true if the authentication state was valid for the current login manager, false
     * otherwise.
     */
    fun isRefreshable(authState: AppAuthState): Boolean

    /**
     * Start to perform a login attempt. This may be asynchronous. At the end of the
     * login attempt, call [LoginActivity.loginSucceeded] or
     * [LoginActivity.loginFailed].
     *
     * @param authState current authentication state
     */
    suspend fun start(authState: AppAuthState)

    /**
     * Initialization at the end of [LoginActivity.onCreate].
     * @param activity the activity that was created
     * @return whether the current login manager will act on this call
     */
    fun onActivityCreate(activity: Activity): Boolean

    /**
     * Invalidate the authentication state
     * @param authState current authentication state
     * @param disableRefresh disable any refresh capability that the current state may have
     * @return invalidated state, or `null` if the current loginManager cannot invalidate
     * the token.
     */
    suspend fun invalidate(authState: AppAuthState.Builder, disableRefresh: Boolean)

    /**
     * Register a source.
     * @param authState authentication state
     * @param source source metadata to register
     * @param success callback to call on success
     * @param failure callback to call on failure
     * @return true if the current LoginManager can handle the registration, false otherwise.
     * @throws AuthenticationException if the manager cannot log in
     */
    @Throws(AuthenticationException::class)
    suspend fun registerSource(
        authState: AppAuthState.Builder,
        source: SourceMetadata,
        success: suspend (AppAuthState, SourceMetadata) -> Unit,
        failure: suspend (Exception?) -> Unit
    ): Boolean

    fun onDestroy()

    /**
     * Update a source.
     * @param appAuth authentication state
     * @param source source metadata to register
     * @param success callback to call on success
     * @param failure callback to call on failure
     * @return true if the current LoginManager can handle the update, false otherwise.
     * @throws AuthenticationException if the manager cannot log in
     */
    suspend fun updateSource(
        appAuth: AppAuthState.Builder,
        source: SourceMetadata,
        success: (AppAuthState, SourceMetadata) -> Unit,
        failure: (Exception?) -> Unit
    ): Boolean

    companion object {
        /** HTTP basic authentication.  */
        const val AUTH_TYPE_UNKNOWN = 0
        /** HTTP bearer token.  */
        const val AUTH_TYPE_BEARER = 1
        /** HTTP basic authentication.  */
        const val AUTH_TYPE_HTTP_BASIC = 2
    }
}
