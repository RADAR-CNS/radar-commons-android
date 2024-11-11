package org.radarbase.android.auth

/** Manages persistence of the AppAuthState. */
interface AuthSerialization {
    /**
     * Load the state from this serialization.
     * @return state if it was stored, null otherwise.
     */
    suspend fun load(): AppAuthState?
    /**
     * Store the auth state to this serialization.
     */
    suspend fun store(state: AppAuthState)
    /**
     * Remove the auth state from this serialization.
     */
    suspend fun remove()
}
