package org.radarbase.android.auth

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import kotlinx.coroutines.test.runTest
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.radarbase.android.auth.portal.ManagementPortalClient
import java.util.concurrent.TimeUnit

@RunWith(AndroidJUnit4::class)
class SharedPreferencesAuthSerializationTest {
    private lateinit var state: AppAuthState
    private lateinit var sources: List<SourceMetadata>

    @Before
    fun setUp() {
        sources = listOf(SourceMetadata().apply {
            type = SourceType(1, "radar", "test", "1.0", true)
            expectedSourceName = "something"
        })

        state = AppAuthState {
            token = "abcd"
            tokenType = LoginManager.AUTH_TYPE_BEARER
            projectId = "p"
            userId = "u"
            attributes[ManagementPortalClient.MP_REFRESH_TOKEN_PROPERTY] = "efgh"
            sourceMetadata += sources
            addHeader("Authorization", "Bearer abcd")
            expiration = System.currentTimeMillis() + 10_000L
            isPrivacyPolicyAccepted = true
        }

        testProperties(state)
    }

    @Test
    fun addToPreferences() = runTest{
        val readState = ApplicationProvider.getApplicationContext<Context>().let { context ->
            val authSerializer = SharedPreferencesAuthSerialization(context)
            authSerializer.store(state)
            authSerializer.load()
        }

        assertNotNull(readState)
        testProperties(readState!!)
    }

    private fun testProperties(state: AppAuthState, refreshToken: String = "efgh") {
        assertEquals("abcd", state.token)
        assertEquals(refreshToken, state.getAttribute(ManagementPortalClient.MP_REFRESH_TOKEN_PROPERTY))
        assertEquals("p", state.projectId)
        assertEquals("u", state.userId)
        assertTrue(state.isValidFor(9, TimeUnit.SECONDS))
        assertFalse(state.isValidFor(11, TimeUnit.SECONDS))
        assertEquals(LoginManager.AUTH_TYPE_BEARER.toLong(), state.tokenType.toLong())
        assertEquals("Bearer abcd", state.headers[0].second)
        assertEquals(sources, state.sourceMetadata)
    }
}
