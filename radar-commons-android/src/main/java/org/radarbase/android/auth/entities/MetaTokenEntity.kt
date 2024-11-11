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

import kotlinx.serialization.Serializable
import org.radarbase.android.util.equalTo
import java.util.Objects

@Serializable
class MetaTokenEntity(
    val refreshToken: String,
    val baseUrl: String,
    val privacyPolicyUrl: String
) : AuthResponseEntity {

    override fun equals(other: Any?): Boolean = equalTo(
        other,
        MetaTokenEntity::refreshToken,
        MetaTokenEntity::baseUrl,
        MetaTokenEntity::privacyPolicyUrl
    )

    override fun hashCode(): Int {
        return Objects.hash(refreshToken, baseUrl, privacyPolicyUrl)
    }

    override fun toString(): String {
        return "MetaTokenEntity{" +
                "refreshToken='$refreshToken', " +
                "baseUrl='$baseUrl', " +
                "privacyPolicyUrl='$privacyPolicyUrl'}"
    }
}