package org.radarbase.android.auth

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.json.JSONException
import org.json.JSONObject
import org.radarbase.android.util.buildJson
import org.radarbase.android.util.equalTo
import org.slf4j.LoggerFactory
import java.util.*

@Serializable
class SourceType(
    @SerialName("sourceTypeId")
    val id: Int,
    @SerialName("sourceTypeProducer")
    val producer: String,
    @SerialName("sourceTypeModel")
    val model: String,
    @SerialName("sourceTypeCatalogVersion")
    val catalogVersion: String,
    @SerialName("dynamicRegistration")
    val hasDynamicRegistration: Boolean,
) {
    override fun equals(other: Any?): Boolean = equalTo(
        other,
        SourceType::id,
        SourceType::producer,
        SourceType::model,
        SourceType::catalogVersion,
        SourceType::hasDynamicRegistration,
    )

    override fun hashCode(): Int = Objects.hash(id)

    override fun toString(): String = "SourceType{" +
            "id='$id', " +
            "producer='$producer', " +
            "model='$model', " +
            "catalogVersion='$catalogVersion', " +
            "dynamicRegistration=$hasDynamicRegistration}"
}
