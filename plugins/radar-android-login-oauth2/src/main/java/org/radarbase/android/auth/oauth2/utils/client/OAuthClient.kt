package org.radarbase.android.auth.oauth2.utils.client

import okhttp3.MediaType.Companion.toMediaType
import okhttp3.RequestBody.Companion.toRequestBody
import org.json.JSONException
import org.json.JSONObject
import org.radarbase.android.auth.AppAuthState
import org.radarbase.android.auth.AuthenticationSource
import org.radarbase.android.auth.SourceMetadata
import org.radarbase.android.auth.portal.ConflictException
import org.radarbase.android.auth.portal.GetSubjectParser
import org.radarbase.android.auth.portal.ManagementPortalClient.Companion.SourceNotFoundException
import org.radarbase.android.auth.portal.ManagementPortalClient.Companion.UserNotFoundException
import org.radarbase.android.auth.portal.ManagementPortalClient.Companion.parseSourceRegistration
import org.radarbase.android.auth.portal.ManagementPortalClient.Companion.sourceRegistrationBody
import org.radarbase.android.auth.portal.ManagementPortalClient.Companion.sourceUpdateBody
import org.radarbase.config.ServerConfig
import org.radarbase.producer.AuthenticationException
import org.radarbase.producer.rest.RestClient
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.IOException

/**
 * OAuthClient is responsible for interacting with the Management Portal (MP) through OAuth2-based REST requests.
 * It provides functionality for registering and updating sources, as well as fetching subjects from the MP.
 *
 * @param serverConfig the server configuration in form of [ServerConfig] for the Management Portal.
 * @param client an optional RestClient instance for making requests. If not provided, a new RestClient is created.
 */
class OAuthClient(serverConfig: ServerConfig, client: RestClient? = null) {

    /**
     * The HTTP client for interacting with the Management Portal.
     * Built using the provided server configuration and an optional RestClient.
     */
    val httpClient: RestClient = (client?.newBuilder() ?: RestClient.newClient())
        .server(serverConfig)
        .build()

    /** Register a source with the Management Portal.  */
    @Throws(IOException::class, JSONException::class)
    fun registerSource(auth: AppAuthState, source: SourceMetadata): SourceMetadata = handleSourceUpdateRequest(
        auth,
        "api/subjects/${auth.userId}/sources",
        sourceRegistrationBody(source),
        source,
    )

    /** Updates a source from Management Portal.  */
    @Throws(IOException::class, JSONException::class)
    fun updateSource(auth: AppAuthState, source: SourceMetadata): SourceMetadata = handleSourceUpdateRequest(
        auth,
        "api/subjects/${auth.userId}/sources/${source.sourceName}",
        sourceUpdateBody(source),
        source,
    )

    /**
     * Requests subject information from the Management Portal based on the current AppAuthState.
     *
     * @param state the current AppAuthState containing userId and OAuth credentials.
     * @return an updated AppAuthState containing subject information from the Management Portal.
     * @throws IOException if the userId is null or if there is a network error or invalid response from the Management Portal.
     */
    @Throws(IOException::class)
    fun requestSubjectsFromMp(state: AppAuthState): AppAuthState {
        if (state.userId == null) {
            throw IOException("AppAuthState doesn't contains userId")
        }
        logger.debug("Fetching Subject from MP via oAuth flow")

        httpClient.requestBuilder("/api/subjects/${state.userId}")
            .headers(state.okHttpHeaders)
            .header(ACCEPT_HEADER, APPLICATION_JSON)
            .build().also {
                httpClient.request(it).use { res ->
                    val body = RestClient.responseBody(res)
                        ?.takeIf { it.isNotEmpty() }
                        ?: throw IOException("Received empty response for subject from MP")

                    when (res.code) {
                        in 400..599 -> throw IOException("Failed to make request; response $body")
                    }

                    return GetSubjectParser(
                        state,
                        AuthenticationSource.SELF_ENROLMENT_PORTAL
                    ).parse(body)
                }
            }
    }

    /**
     * Helper function to handle requests for source registration or updates.
     *
     * @param auth the current authentication state containing user information and OAuth credentials.
     * @param relativePath the relative API path to either register or update a source.
     * @param requestBody the request body containing source data in JSON format.
     * @param source the metadata of the source to register or update.
     * @return the source metadata returned by the Management Portal.
     * @throws IOException if there is a network error, a conflict, or the source/user is not found.
     */
    private fun handleSourceUpdateRequest(
        auth: AppAuthState,
        relativePath: String,
        requestBody: JSONObject,
        source: SourceMetadata,
    ): SourceMetadata {
        val request = httpClient.requestBuilder(relativePath)
            .post(requestBody.toString().toRequestBody(APPLICATION_JSON_TYPE))
            .headers(auth.okHttpHeaders)
            .header("Content-Type", APPLICATION_JSON_UTF8)
            .header("Accept", APPLICATION_JSON)
            .build()

        return httpClient.request(request).use { response ->
            val responseBody = RestClient.responseBody(response)
                ?.takeIf { it.isNotEmpty() }
                ?: throw IOException("Source registration with the ManagementPortal did not yield a response body.")

            when (response.code) {
                401 -> throw AuthenticationException("Authentication failure with the ManagementPortal: $responseBody")
                403 -> throw UnsupportedOperationException("Not allowed to update source data: $responseBody")
                404 -> {
                    val error = JSONObject(responseBody)
                    if (error.getString("message").contains("source", true)) {
                        throw SourceNotFoundException("Source ${source.sourceId} is no longer registered with the ManagementPortal: $responseBody")
                    } else {
                        throw UserNotFoundException("User ${auth.userId} is no longer registered with the ManagementPortal: $responseBody")
                    }
                }

                409 -> throw ConflictException("Source registration conflicts with existing source registration: $responseBody")
                in 400..599 -> throw IOException("Cannot complete source registration with the ManagementPortal: $responseBody, using request $requestBody")
            }

            parseSourceRegistration(responseBody, source)
            source
        }
    }


    companion object {
        private val logger: Logger = LoggerFactory.getLogger(OAuthClient::class.java)
        private const val ACCEPT_HEADER = "Accept"
        private const val APPLICATION_JSON = "application/json"
        private const val APPLICATION_JSON_UTF8 = "$APPLICATION_JSON; charset=utf-8"
        private val APPLICATION_JSON_TYPE = APPLICATION_JSON_UTF8.toMediaType()
    }
}