package org.radarbase.android.auth

import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.radarbase.android.auth.portal.ManagementPortalClient
import java.util.concurrent.TimeUnit

class AppAuthStateTest {
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

    private fun testProperties(
        state: AppAuthState,
        refreshToken: String = "efgh",
        projectId: String = "p",
        tokenType: Int = LoginManager.AUTH_TYPE_BEARER,
        headerValues: List<String> = listOf("Bearer abcd")
    ) {
        assertEquals("abcd", state.token)
        assertEquals(refreshToken, state.getAttribute(ManagementPortalClient.MP_REFRESH_TOKEN_PROPERTY))
        assertEquals(projectId, state.projectId)
        assertEquals("u", state.userId)
        assertTrue(state.isValidFor(9, TimeUnit.SECONDS))
        assertFalse(state.isValidFor(11, TimeUnit.SECONDS))
        assertEquals(tokenType.toLong(), state.tokenType.toLong())
        state.headers.forEachIndexed { index, pair: Pair<String, String> ->
            assertEquals(pair.second, headerValues[index])
        }
        assertEquals("Bearer abcd", state.headers[0].second)
        assertEquals(sources, state.sourceMetadata)
    }

    @Test
    fun newBuilder() = runTest{
        val newHeaders = mutableMapOf<String, String>().apply {
            put("Accept", "application/json")
            put("Username", "user")
            put("Password", "pass")
        }
        val updatedHeaders = mutableMapOf<String, String>().apply {
            putAll(state.headers)
            putAll(newHeaders)
        }
        val builtState = state.alter {
            attributes[ManagementPortalClient.MP_REFRESH_TOKEN_PROPERTY] = "else"
            projectId = "Test Project"
            tokenType = LoginManager.AUTH_TYPE_HTTP_BASIC
            newHeaders.forEach {
                headers += it.toPair()
            }
        }

        testProperties(
            builtState,
            refreshToken = "else",
            projectId = "Test Project",
            tokenType = LoginManager.AUTH_TYPE_HTTP_BASIC,
            headerValues = updatedHeaders.values.toList()
        )
        testProperties(state)
    }
}
