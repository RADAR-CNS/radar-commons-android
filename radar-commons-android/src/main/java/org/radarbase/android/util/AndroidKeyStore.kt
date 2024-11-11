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

package org.radarbase.android.util

import android.security.keystore.KeyGenParameterSpec
import java.security.Key
import java.security.KeyStore
import javax.crypto.KeyGenerator

object AndroidKeyStore {
    fun getOrCreateSecretKey(name: String, algorithm: String, purposes: Int, generatorBuilder: KeyGenParameterSpec.Builder.() -> Unit = {}): Key {
        val keyStore = KeyStore.getInstance(ANDROID_KEY_STORE)
        keyStore.load(null)
        val key = keyStore.getKey(name, null)
        return if (key != null) {
            key
        } else {
            val keyGenerator = KeyGenerator.getInstance(algorithm, ANDROID_KEY_STORE)
            keyGenerator.init(KeyGenParameterSpec.Builder(name, purposes)
                    .apply(generatorBuilder)
                    .build())
            keyGenerator.generateKey()
        }
    }

    private const val ANDROID_KEY_STORE = "AndroidKeyStore"
}
