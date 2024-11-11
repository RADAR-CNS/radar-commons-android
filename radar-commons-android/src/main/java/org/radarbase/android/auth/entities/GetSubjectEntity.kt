/*
 * Copyright 2017 The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarbase.android.auth.entities

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.radarbase.android.auth.SourceMetadata
import org.radarbase.android.util.equalTo
import java.util.Objects

@Serializable
data class GetSubjectEntity(
    @SerialName("project")
    val mpProject: ManagementPortalProject,
    @SerialName("sources")
    val mpSources: List<SourceMetadata>,
    @SerialName("login")
    val userId: String,
    @SerialName("attributes")
    val attributes: Map<String, String>,
    @SerialName("externalId")
    val radarExternalId: String?,
    @SerialName("externalLink")
    val radarExternalUrl: String?
) {
    override fun equals(other: Any?): Boolean = equalTo(
        other,
        GetSubjectEntity::userId,
        GetSubjectEntity::mpProject,
        GetSubjectEntity::mpSources,
        GetSubjectEntity::userId,
        GetSubjectEntity::attributes,
        GetSubjectEntity::radarExternalId,
        GetSubjectEntity::radarExternalUrl
    )

    override fun hashCode(): Int {
        return Objects.hash(userId, mpProject)
    }

    override fun toString(): String {
        return "GetSubjectEntity(" +
                "mpProject=$mpProject, " +
                "mpSources=$mpSources, " +
                "userId='$userId', " +
                "attributes=$attributes, " +
                "radarExternalId=$radarExternalId, " +
                "radarExternalUrl=$radarExternalUrl" +
                ")"
    }
}