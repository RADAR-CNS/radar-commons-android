package org.radarbase.android.auth.portal

import org.json.JSONObject
import org.radarbase.android.auth.AppAuthState
import org.radarbase.android.auth.AuthStringParser

import java.io.IOException

class SubjectTokenParser(private val client: ManagementPortalClient, state: AppAuthState.Builder) :
    AuthStringParser {
    private val accessTokenParser: AccessTokenParser = AccessTokenParser(state)

    @Throws(IOException::class)
    override suspend fun parse(value: JSONObject): AppAuthState.Builder {
        val newState = this.accessTokenParser.parse(value)
        return client.getSubject(newState, GetSubjectParser(newState))
    }
}
