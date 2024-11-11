package org.radarbase.android.auth

import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonTransformingSerializer

class SourceMetadataSerializer : JsonTransformingSerializer<SourceMetadata>(SourceMetadata.serializer()) {
    override fun transformSerialize(element: JsonElement): JsonElement {
        if (element !is JsonObject) return element
        if (!element.containsKey("type")) return element
        val type = element["type"] as? JsonObject ?: return element

        return JsonObject(
            buildMap {
                element.filterTo(this) { it.key != "type" }
                putAll(type)
            }
        )
    }

    override fun transformDeserialize(element: JsonElement): JsonElement {
        if (element !is JsonObject) return element

        return JsonObject(
            buildMap {
                val sourceMetadataObj = this
                put("type", JsonObject(buildMap {
                    val sourceTypeObj = this
                    for ((key, value) in element) {
                        if (key.startsWith("sourceType") || key == "dynamicRegistration") {
                            sourceTypeObj[key] = value
                        } else {
                            sourceMetadataObj[key] = value
                        }
                    }
                }))
            }
        )
    }
}
