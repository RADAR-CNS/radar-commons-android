package org.radarbase.android.auth.portal

import org.json.JSONArray
import org.json.JSONException
import org.json.JSONObject
import org.radarbase.android.auth.AppAuthState
import org.radarbase.android.auth.AuthStringParser
import org.radarbase.android.auth.SourceMetadata
import org.radarbase.android.auth.SourceType
import org.radarbase.android.auth.portal.ManagementPortalLoginManager.Companion.SOURCE_TYPE
import org.radarbase.android.util.asJSONObjectSequence
import org.radarbase.android.util.optNonEmptyString
import org.radarbase.android.util.takeTrimmedIfNotEmpty
import org.radarbase.android.util.toStringMap
import org.slf4j.LoggerFactory
import java.io.IOException
import kotlin.collections.ArrayList

class GetSubjectParser(private val state: AppAuthState.Builder) : AuthStringParser {

    @Throws(IOException::class)
    override suspend fun parse(value: JSONObject): AppAuthState.Builder {
        try {
            val project = value.getJSONObject("project")
            val sources = value.getJSONArray("sources")

            val types = parseSourceTypes(project)

            return state.apply {
                sourceTypes.clear()
                sourceTypes += types
                sourceMetadata.clear()
                sourceMetadata += parseSources(types, sources)
                userId = parseUserId(value)
                projectId = parseProjectId(project)
                needsRegisteredSources = true
                authenticationSource = SOURCE_TYPE

                value.opt("attributes")?.let { attrObjects ->
                    if (attrObjects is JSONArray) {
                        attrObjects
                            .asJSONObjectSequence()
                            .forEach { attrObject ->
                                attributes[attrObject.getString("key")] =
                                    attrObject.getString("value")
                            }
                    } else if (attrObjects is JSONObject) {
                        attributes += attrObjects.toStringMap()
                    }
                }
                value.optNonEmptyString("externalId")?.let {
                    attributes[RADAR_EXTERNAL_ID] = it
                }
                value.optNonEmptyString("externalLink")?.let {
                    attributes[RADAR_EXTERNAL_URL] = it
                }
            }
        } catch (e: JSONException) {
            throw IOException(
                "ManagementPortal did not give a valid response: $value", e
            )
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(GetSubjectParser::class.java)

        const val RADAR_EXTERNAL_ID = "radar_external_id"
        const val RADAR_EXTERNAL_URL = "radar_external_url"

        @Throws(JSONException::class)
        internal fun parseSourceTypes(project: JSONObject): List<SourceType> {
            val sourceTypes = project.getJSONArray("sourceTypes")

            return sourceTypes
                .asJSONObjectSequence()
                .mapTo(ArrayList(sourceTypes.length())) {
                    it.run {
                        logger.debug("Parsing source type {}", this)
                        SourceType(
                            id = getInt("id"),
                            producer = getString("producer"),
                            model = getString("model"),
                            catalogVersion = getString("catalogVersion"),
                            hasDynamicRegistration = getBoolean("canRegisterDynamically"),
                        )
                    }
                }
        }

        @Throws(JSONException::class)
        internal fun parseSources(
            sourceTypes: List<SourceType>,
            sources: JSONArray
        ): List<SourceMetadata> {

            val actualSources = sources
                .asJSONObjectSequence()
                .mapNotNullTo(ArrayList(sources.length())) { sourceObj ->
                    val id = sourceObj.getString("sourceId")
                    if (!sourceObj.optBoolean("assigned", true)) {
                        logger.debug("Skipping unassigned source {}", id)
                    }
                    val sourceTypeId = sourceObj.getInt("sourceTypeId")
                    val sourceType = sourceTypes.find { it.id == sourceTypeId }

                    if (sourceType != null) {
                        SourceMetadata(sourceType).apply {
                            expectedSourceName = sourceObj.optNonEmptyString("expectedSourceName")
                            sourceName = sourceObj.optNonEmptyString("sourceName")
                            sourceId = id
                            attributes = sourceObj.optJSONObject("attributes")?.toStringMap()
                                ?: emptyMap()
                        }
                    } else {
                        logger.error("Source {} type {} not recognized", id, sourceTypeId)
                        null
                    }
                }

            logger.info("Sources from Management Portal: {}", actualSources)
            return actualSources
        }

        @Throws(JSONException::class)
        internal fun parseProjectId(project: JSONObject): String = project.getString("projectName")

        /**
         * Parse the user ID from a subject response jsonObject.
         * @param jsonObject response JSON jsonObject of a subject call
         * @return user ID
         * @throws JSONException if the given JSON jsonObject does not contain a login property
         */
        @Throws(JSONException::class)
        internal fun parseUserId(jsonObject: JSONObject): String {
            return jsonObject.getString("login")
        }

        val AppAuthState.externalUserId: String?
            get() = attributes[RADAR_EXTERNAL_ID]

        val AppAuthState.humanReadableUserId: String?
            get() = attributes.values
                .find { it.equals("Human-readable-identifier", ignoreCase = true) }
                ?.takeTrimmedIfNotEmpty()
                ?: userId
    }
}
