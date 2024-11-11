package org.radarbase.android.auth

import kotlinx.serialization.Serializable
import org.json.JSONException
import org.json.JSONObject
import org.radarbase.android.RadarService.Companion.sanitizeIds
import org.radarbase.android.util.*
import org.radarbase.android.util.optNonEmptyString
import org.radarbase.android.util.toJson
import org.radarbase.android.util.toStringMap
import org.radarbase.util.Strings
import org.slf4j.LoggerFactory
import java.util.*
import java.util.regex.Pattern

@Serializable(with = SourceMetadataSerializer::class)
data class SourceMetadata(
    var type: SourceType? = null,
    var sourceId: String? = null,
    var sourceName: String? = null,
    var expectedSourceName: String? = null,
    var attributes: Map<String, String> = mapOf()
) {
    fun deduplicateType(types: MutableCollection<SourceType>): SourceMetadata {
        val currentType = type ?: return this
        val storedType = types.find { it.id == currentType.id }
        return if (storedType != null) {
            copy(type = storedType)
        } else {
            types += currentType
            this
        }
    }

    override fun equals(other: Any?) = equalTo(
        other,
        SourceMetadata::type,
        SourceMetadata::sourceId,
        SourceMetadata::sourceName,
        SourceMetadata::expectedSourceName,
        SourceMetadata::attributes,
    )

    override fun hashCode(): Int = Objects.hash(type, sourceId)

    override fun toString(): String = "SourceMetadata{" +
        "type=$type, " +
        "sourceId='$sourceId', " +
        "sourceName='$sourceName', " +
        "expectedSourceName='$expectedSourceName', " +
        "attributes=$attributes'}"

    fun matches(other: SourceMetadata): Boolean {
        val type = type ?: return false
        val otherType = other.type ?: return false

        return sourceId == other.sourceId
                || type == otherType
                || (type.producer.equals(otherType.producer, ignoreCase = true)
                    && type.model.equals(otherType.model, ignoreCase = true))
    }

    fun matches(vararg names: String?): Boolean {
        val actualNames = names.filterNotNull()
        if (actualNames.isEmpty()) {
            return false
        }
        val expectedSourceName = expectedSourceName ?: return true
        val hasMatch = expectedSourceName
            .split(',')
            .sanitizeIds()
            .any { pattern ->
                val regex = pattern.toRegex(setOf(RegexOption.IGNORE_CASE, RegexOption.LITERAL))
                actualNames.any { regex.containsMatchIn(it) }
            }

        return if (hasMatch) {
            true
        } else {
            logger.warn("Source names {} were not matched by {}", actualNames, expectedSourceName)
            false
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(SourceMetadata::class.java)
    }
}
