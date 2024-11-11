package org.radarbase.android.util

import android.app.PendingIntent
import android.content.Context
import android.os.Build
import org.json.JSONArray
import org.json.JSONObject
import org.slf4j.Logger

fun String.takeTrimmedIfNotEmpty(): String? = trim { it <= ' ' }
    .takeUnless(String::isEmpty)

fun Int.toPendingIntentFlag(mutable: Boolean = false) = this or when {
    mutable && Build.VERSION.SDK_INT >= Build.VERSION_CODES.S -> PendingIntent.FLAG_MUTABLE
    !mutable -> PendingIntent.FLAG_IMMUTABLE
    else -> 0
}

@Suppress("UNCHECKED_CAST")
inline fun <reified T> Context.applySystemService(
    type: String,
    callback: (T) -> Boolean
): Boolean? {
    return (getSystemService(type) as T?)?.let(callback)
}


internal fun Map<String, Any>.toJson(): JSONObject = buildJson {
    forEach { (k, v) -> this@buildJson.put(k, v) }
}

internal inline fun buildJson(config: JSONObject.() -> Unit): JSONObject {
    return JSONObject().apply(config)
}

internal inline fun buildJsonArray(config: JSONArray.() -> Unit): JSONArray {
    return JSONArray().apply(config)
}

internal fun JSONObject.optNonEmptyString(key: String): String? =
    if (isNull(key)) null else optString(key).takeTrimmedIfNotEmpty()?.takeIf { it != "null" }

internal fun JSONObject.toStringMap(): Map<String, String> = buildMap {
    this@toStringMap.keys().forEach { key ->
        this@buildMap.put(key, getString(key))
    }
}

inline fun <T> Logger.runSafeOrNull(block: () -> T) = try {
    block()
} catch (ex: Exception) {
    this.error("Failed to complete task", ex)
    null
}

internal fun JSONArray.asJSONObjectSequence(): Sequence<JSONObject> = sequence {
    for (i in 0 until length()) {
        yield(getJSONObject(i))
    }
}

internal fun JSONObject.putAll(map: Map<String, Any?>) {
    for (entry in map) {
        put(entry.key, entry.value)
    }
}

internal inline fun <reified T : Any> T.equalTo(other: Any?, vararg fields: T.() -> Any?): Boolean {
    if (this === other) return true
    if (other == null || javaClass !== other.javaClass) return false
    other as T
    return fields.all { field -> field() == other.field() }
}

//
//inline fun <T, R> LiveData<T>.map(crossinline transform: (T) -> R): LiveData<R> =
//    Transformations.map(this) { transform(it) }
//
//fun <T> LiveData<T>.distinctUntilChanged(): LiveData<T> =
//    Transformations.distinctUntilChanged(this)
//
//inline fun <T, R> LiveData<T>.switchMap(crossinline transform: (T) -> LiveData<R>): LiveData<R> =
//    Transformations.switchMap(this) { transform(it) }
