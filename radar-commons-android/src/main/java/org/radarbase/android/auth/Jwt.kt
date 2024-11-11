package org.radarbase.android.auth

import android.util.Base64
import android.util.Base64.NO_PADDING
import android.util.Base64.NO_WRAP
import org.json.JSONException
import org.json.JSONObject

class Jwt(val header: JSONObject, val body: JSONObject) {
    companion object {
        @Throws(JSONException::class)
        fun parse(token: String): Jwt {
            val parts = token.split('.')
            require(parts.size == 3) { "Argument is not a valid JSON web token. Need 3 parts but got " + parts.size }
            val header = parts[0].decodeBase64Json()
            val body = parts[1].decodeBase64Json()
            return Jwt(header, body)
        }

        @Throws(JSONException::class)
        private fun String.decodeBase64Json(): JSONObject {
            val decoded = Base64.decode(this, NO_PADDING or NO_WRAP)
            return JSONObject(String(decoded))
        }
    }
}
