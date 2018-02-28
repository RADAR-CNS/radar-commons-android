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

package org.radarcns.android.auth;

import android.content.Intent;
import android.os.Bundle;
import org.apache.avro.reflect.Nullable;

/** Manage a single login method. */
public interface LoginManager {
    /** HTTP basic authentication. */
    int AUTH_TYPE_UNKNOWN = 0;
    /** HTTP bearer token. */
    int AUTH_TYPE_BEARER = 1;
    /** HTTP basic authentication. */
    int AUTH_TYPE_HTTP_BASIC = 2;

    /**
     * With or without user interaction, refresh the current authentication state. If successful,
     * return the refreshed authentication state, otherwise return null or a stale authentication
     * state.
     * @return refreshed authentication state or null.
     */
    @Nullable
    AppAuthState refresh();

    /**
     * Start to perform a login attempt. This may be asynchronous. At the end of the
     * login attempt, call {@link LoginActivity#loginSucceeded(LoginManager, AppAuthState)} or
     * {@link LoginActivity#loginFailed(LoginManager, Exception)}. If this starts another
     * activity (using the LoginActivity as the active activity), monitor
     * {@link #onActivityResult(int, int, Intent)} for the result.
     */
    void start();

    /**
     * Initialization at the end of {@link LoginActivity#onCreate(Bundle)}.
     */
    void onActivityCreate();

    /**
     * Process an activity result on the {@link LoginActivity}. This may be a noop if this
     * control flow is not used.
     */
    void onActivityResult(int requestCode, int resultCode, Intent data);
}
