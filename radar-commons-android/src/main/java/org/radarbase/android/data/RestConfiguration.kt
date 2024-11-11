package org.radarbase.android.data

import io.ktor.http.Headers
import io.ktor.http.headersOf
import org.radarbase.android.RadarConfiguration
import org.radarbase.android.config.SingleRadarConfiguration
import org.radarbase.android.util.ServerConfigUtil.toServerConfig
import org.radarbase.config.ServerConfig

data class RestConfiguration(
    /** Request headers. */
    var headers: Headers = headersOf(),
    /** Kafka server configuration. If null, no rest sender will be configured. */
    var kafkaConfig: ServerConfig? = null,
    /** Schema registry retriever. */
    var schemaRetrieverUrl: String? = null,
    /** Connection timeout in seconds. */
    var connectionTimeout: Long = 10L,
    /** Whether to try to use GZIP compression in requests. */
    var useCompression: Boolean = false,
    /** Whether to try to use binary encoding in request. */
    var hasBinaryContent: Boolean = false,
    /** Unsafe http connections can be used or secure connections are needed*/
    var unsafeKafka: Boolean = false
) {
    fun configure(config: SingleRadarConfiguration) {
        unsafeKafka = config.getBoolean(RadarConfiguration.UNSAFE_KAFKA_CONNECTION, false)

        kafkaConfig = config.optString(RadarConfiguration.KAFKA_REST_PROXY_URL_KEY)
            ?.toServerConfig(unsafeKafka)

        schemaRetrieverUrl = config.optString(RadarConfiguration.SCHEMA_REGISTRY_URL_KEY)

        hasBinaryContent = config.getBoolean(RadarConfiguration.SEND_BINARY_CONTENT, RadarConfiguration.SEND_BINARY_CONTENT_DEFAULT)
        useCompression = config.getBoolean(RadarConfiguration.SEND_WITH_COMPRESSION, false)
        connectionTimeout = config.getLong(RadarConfiguration.SENDER_CONNECTION_TIMEOUT_KEY, connectionTimeout)
    }
}
