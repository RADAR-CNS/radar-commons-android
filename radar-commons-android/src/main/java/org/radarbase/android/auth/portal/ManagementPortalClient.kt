package org.radarbase.android.auth.portal

import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.plugins.*
import io.ktor.client.plugins.auth.*
import io.ktor.client.plugins.auth.providers.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.request.*
import io.ktor.client.request.forms.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import kotlinx.serialization.json.Json
import org.json.JSONException
import org.json.JSONObject
import org.radarbase.android.auth.AppAuthState
import org.radarbase.android.auth.AuthStringParser
import org.radarbase.android.auth.SourceMetadata
import org.radarbase.android.util.*
import org.radarbase.android.util.buildJson
import org.radarbase.android.util.optNonEmptyString
import org.radarbase.android.util.toJson
import org.radarbase.android.util.toStringMap
import org.radarbase.config.ServerConfig
import org.radarbase.producer.AuthenticationException
import org.slf4j.LoggerFactory
import java.io.IOException
import java.lang.RuntimeException
import java.net.MalformedURLException

/**
 * Client for interacting with the Management Portal to authenticate and manage users and devices.
 * This class handles requests related to authentication, user retrieval, source registration, and
 * source updates.
 *
 * This client uses the Ktor HTTP client for making requests, and handles different types of
 * JSON responses from the Management Portal.
 *
 * It expects a [ServerConfig] instance to define the Management Portal URL and handles
 * basic authentication using client credentials (client ID and secret).
 *
 * Operations in this class include:
 *
 *     Getting a refresh token from a meta-token URL.
 *     Fetching subject information such as project details, available source types, and
 *     assigned sources.
 *     Registering and updating sources linked to a subject.
 * </ul>
 */

class ManagementPortalClient(
    managementPortal: ServerConfig,
    private val clientId: String,
    private val clientSecret: String,
    client: HttpClient,
) {
    val client: HttpClient = client.config {
        install(ContentNegotiation) {
            json(Json {
                ignoreUnknownKeys = true
                coerceInputValues = true
            })
        }
        defaultRequest {
            url(managementPortal.urlString)
            accept(ContentType.Application.Json)
        }
    }

    /**
     * Get refresh-token from meta-token url.
     * @param metaTokenUrl current token url
     * @param parser string parser
     * @throws IOException if the management portal could not be reached or it gave an erroneous
     * response.
     */
    @Throws(IOException::class)
    suspend fun getRefreshToken(metaTokenUrl: String, parser: AuthStringParser): AppAuthState.Builder {
        val request = client.prepareRequest(metaTokenUrl) {
            logger.debug("Requesting refreshToken with token-url {}", metaTokenUrl)
        }

        return handleRequest(request, parser)
    }

    /**
     * Get subject information from the Management portal. This includes project ID, available
     * source types and assigned sources.
     * @param state current authentication state
     * @throws IOException if the management portal could not be reached or it gave an erroneous
     * response.
     */
    @Throws(IOException::class)
    suspend fun getSubject(state: AppAuthState.Builder, parser: AuthStringParser): AppAuthState.Builder {
        if (state.userId == null) {
            throw IOException("Authentication state does not contain user ID")
        }
        val ktorHeaders: Headers = Headers.build {
            state.headers.forEach { (k, v) -> append(k, v) }
        }

        val request = client.prepareRequest("api/subjects/${state.userId}") {
            headers {
                appendAll(ktorHeaders)
                append(HttpHeaders.Accept, APPLICATION_JSON)
            }
        }

        logger.info("Requesting subject {} with parseHeaders {}", state.userId,
                ktorHeaders)

        return handleRequest(request, parser)
    }

    /**
     * Registers a source in the Management Portal for the specified subject.
     *
     * @param auth the current authentication state
     * @param source the [SourceMetadata] of the source to register
     * @return the registered [SourceMetadata], with updated information
     * @throws IOException if the request fails
     * @throws JSONException if the response cannot be parsed as valid JSON
     */
    @Throws(IOException::class, JSONException::class)
    suspend fun registerSource(auth: AppAuthState, source: SourceMetadata): SourceMetadata =
        handleSourceUpdateRequest(
        auth,
        "api/subjects/${auth.userId}/sources",
        sourceRegistrationBody(source),
        source,
    )

    /**
     * Updates the metadata of a registered source in the Management Portal.
     *
     * @param auth the current authentication state
     * @param source the [SourceMetadata] of the source to update
     * @return the [SourceMetadata], with updated information
     * @throws IOException if the request fails
     * @throws JSONException if the response cannot be parsed as valid JSON
     */
    @Throws(IOException::class, JSONException::class)
    suspend fun updateSource(auth: AppAuthState, source: SourceMetadata): SourceMetadata =
        handleSourceUpdateRequest(
        auth,
        "api/subjects/${auth.userId}/sources/${source.sourceName}",
        sourceUpdateBody(source),
        source,
    )

    private suspend fun handleSourceUpdateRequest(
        auth: AppAuthState,
        relativePath: String,
        requestBody: JSONObject,
        source: SourceMetadata,
    ): SourceMetadata {
        val response = client.post(relativePath) {
            contentType(ContentType.Application.Json)
            headers {
                appendAll(auth.ktorHeaders)
                append(HttpHeaders.ContentType, APPLICATION_JSON)
                append(HttpHeaders.Accept, APPLICATION_JSON)
            }
            setBody(requestBody)
        }
        val responseBody = response.bodyAsText()
        if (responseBody.isEmpty()) {
            throw IOException("Source registration with the ManagementPortal did not yield a response body.")
        }

        if (!response.status.isSuccess()) {
            when (response.status) {
                HttpStatusCode.Unauthorized -> throw AuthenticationException("Authentication failure with the ManagementPortal: $responseBody")
                HttpStatusCode.Forbidden -> throw UnsupportedOperationException("Not allowed to update source data: $responseBody")
                HttpStatusCode.NotFound -> {
                    val error = JSONObject(responseBody)
                    if (error.getString("message").contains("source", true)) {
                        throw SourceNotFoundException("Source ${source.sourceId} is no longer registered with the ManagementPortal: $responseBody")
                    } else {
                        throw UserNotFoundException("User ${auth.userId} is no longer registered with the ManagementPortal: $responseBody")
                    }
                }
                HttpStatusCode.Conflict -> throw ConflictException("Source registration conflicts with existing source registration: $responseBody")
                else -> throw IOException("Cannot complete source registration with the ManagementPortal: $responseBody, using request $requestBody")
            }
        }

        parseSourceRegistration(responseBody, source)
        return source
    }

    /**
     * Exchanges access-token from refresh-token using the refresh token stored in the [AppAuthState.Builder].
     *
     * @param authState the current [AppAuthState] builder
     * @param parser an [AuthStringParser] specifically [AccessTokenParser]  to parse the response
     * @return the updated [AppAuthState.Builder] with the access token, updated headers and source information
     * @throws IOException if the request fails or the refresh token is missing
     */
    @Throws(IOException::class)
    suspend fun refreshToken(authState: AppAuthState.Builder, parser: AuthStringParser): AppAuthState.Builder {
        try {
            val refreshToken = requireNotNull(authState.attributes[MP_REFRESH_TOKEN_PROPERTY]) {
                "No refresh token found"
            }

            val request = client.preparePost("oauth/token") {
                setBody(formData {
                    append("grant_type", "refresh_token")
                    append("refresh_token", refreshToken)
                })
                basicAuth(clientId, clientSecret)
            }

            return handleRequest(request, parser)
        } catch (e: MalformedURLException) {
            throw IllegalStateException("Failed to create request from ManagementPortal url", e)
        }

    }

    /**
     * Internal method to handle requests and parse responses using a generic parser.
     *
     * @param <T> the type of the response object
     * @param request the prepared [HttpStatement] request
     * @param parser the parser ([AuthStringParser]]) for processing the JSON response
     * @return the parsed response of type [T]
     * @throws IOException if the request fails or the response cannot be processed
     */
    @Throws(IOException::class)
    private suspend fun <T> handleRequest(request: HttpStatement, parser: Parser<JSONObject, T>): T {
        return request.execute { response ->
            val body: JSONObject = JSONObject(response.bodyAsText())

            if (response.status == HttpStatusCode.Unauthorized) {
                throw AuthenticationException("QR code is invalid: $body")
            }
            if (!response.status.isSuccess()) {
                throw IOException("Failed to make request; response $body")
            }

            parser.parse(body)
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(ManagementPortalClient::class.java)

        const val SOURCES_PROPERTY = "org.radarcns.android.auth.portal.ManagementPortalClient.sources"
        const val SOURCE_IDS_PROPERTY = "org.radarcns.android.auth.portal.ManagementPortalClient.sourceIds"
        const val MP_REFRESH_TOKEN_PROPERTY = "org.radarcns.android.auth.portal.ManagementPortalClient.refreshToken"
        private const val APPLICATION_JSON = "application/json"
        private val illegalSourceCharacters = "[^_'.@A-Za-z0-9- ]+".toRegex()

        @Throws(JSONException::class)
        internal fun sourceRegistrationBody(source: SourceMetadata): JSONObject {
            val requestBody = JSONObject()

            val typeId = requireNotNull(source.type?.id) { "Cannot register source without a type" }
            requestBody.put("sourceTypeId", typeId)

            source.sourceName?.let {
                val sourceName = it.replace(illegalSourceCharacters, "-")
                requestBody.put("sourceName", sourceName)
                logger.info("Add {} as sourceName", sourceName)
            }
            val sourceAttributes = source.attributes
            if (sourceAttributes.isNotEmpty()) {
                requestBody.put("attributes", sourceAttributes.toJson())
            }
            return requestBody
        }

        @Throws(JSONException::class)
        internal fun sourceUpdateBody(source: SourceMetadata): JSONObject = buildJson {
            putAll(source.attributes)
        }

        /**
         * Parse the response of a subject/source registration.
         * @param body registration response body
         * @param source existing source to update with server information.
         * @throws JSONException if the provided body is not valid JSON with the correct properties
         */
        @Throws(JSONException::class)
        internal fun parseSourceRegistration(body: String, source: SourceMetadata) {
            logger.debug("Parsing source from {}", body)
            val responseObject = JSONObject(body)
            source.apply {
                sourceId = responseObject.getString("sourceId")
                sourceName = responseObject.optNonEmptyString("sourceName") ?: source.sourceId
                expectedSourceName = responseObject.optNonEmptyString("expectedSourceName")
                attributes = responseObject.optJSONObject("attributes")?.toStringMap()
                    ?: emptyMap()
            }
        }

        class SourceNotFoundException(message: String) : RuntimeException(message)
        class UserNotFoundException(message: String) : RuntimeException(message)
    }
}
