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

import android.content.Context
import android.content.Context.MODE_PRIVATE
import android.os.Build
import android.security.keystore.KeyProperties.PURPOSE_SIGN
import android.util.Base64
import java.nio.ByteBuffer
import java.security.InvalidKeyException
import java.security.Key
import java.security.NoSuchAlgorithmException
import java.security.SecureRandom
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

/**
 * Hash generator that uses the HmacSHA256 algorithm to hash data. This algorithm ensures that
 * it is very very hard to guess what data went in. The fixed key to the algorithm is stored in
 * the SharedPreferences. As long as the key remains there, a given input string will always
 * return the same output hash.
 *
 *
 * HashGenerator must be used from a single thread or synchronized externally.
 * This persists the hash.key property in the given preferences.
 */
class HashGenerator(
    context: Context,
    private val name: String,
) {
    private val sha256: Mac
    private val hashBuffer = ByteArray(4)
    private val preferences = context.getSharedPreferences(name, MODE_PRIVATE)

    init {
        try {
            sha256 = Mac.getInstance(HMAC_SHA256)
            sha256.init(loadKey())
        } catch (ex: NoSuchAlgorithmException) {
            throw IllegalStateException("Cannot retrieve hashing algorithm", ex)
        } catch (ex: InvalidKeyException) {
            throw IllegalStateException("Encoding is invalid", ex)
        }

    }

    private fun loadKey(): Key {
        val b64Salt = preferences.getString(HASH_KEY, null)
        return if (b64Salt != null) {
            SecretKeySpec(Base64.decode(b64Salt, Base64.NO_WRAP), HMAC_SHA256)
        } else {
            AndroidKeyStore.getOrCreateSecretKey("$name.$HASH_KEY", HMAC_SHA256, PURPOSE_SIGN)
        }
    }

    /** Create a unique hash for a given target.  */
    fun createHash(target: Int): ByteArray {
        hashBuffer.put(0, target)
        return sha256.doFinal(hashBuffer)
    }

    /** Create a unique hash for a given target.  */
    fun createHash(target: String): ByteArray = sha256.doFinal(target.toByteArray())

    /**
     * Create a unique hash for a given target. Internally this calls
     * [.createHash].
     */
    fun createHashByteBuffer(target: Int): ByteBuffer = ByteBuffer.wrap(createHash(target))

    /**
     * Create a unique hash for a given target. Internally this calls
     * [.createHash].
     */
    fun createHashByteBuffer(target: String): ByteBuffer = ByteBuffer.wrap(createHash(target))

    companion object {
        private const val HASH_KEY = "hash.key"
        private const val HMAC_SHA256 = "HmacSHA256"

        /** Write an int to given bytes with little-endian encoding, starting from startIndex.  */
        fun ByteArray.put(startIndex: Int, value: Int) {
            this[startIndex] = (value shr 24 and 0xFF).toByte()
            this[startIndex + 1] = (value shr 16 and 0xFF).toByte()
            this[startIndex + 2] = (value shr 8 and 0xFF).toByte()
            this[startIndex + 3] = (value and 0xFF).toByte()
        }
    }
}
