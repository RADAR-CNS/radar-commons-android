package org.radarbase.util

import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import java.util.regex.Pattern

/**
 * Utility class for string operations, including pattern matching and
 * character encoding conversion. This class has been adapted from the
 * `radar-commons` library version 0.x.x for compatibility with the current code.
 */
object Strings {
    private val UTF_8: Charset = StandardCharsets.UTF_8
    private val HEX_ARRAY = "0123456789ABCDEF".toCharArray()

    /**
     * For each string, compiles a pattern that checks if it is contained in another string in a
     * case-insensitive way.
     */
    fun containsPatterns(contains: Collection<String>): Array<Pattern> {
        return Array(contains.size) { index -> containsIgnoreCasePattern(contains.elementAt(index)) }
    }

    /**
     * Compiles a pattern that checks if it is contained in another string in a case-insensitive
     * way.
     */
    fun containsIgnoreCasePattern(containsString: String): Pattern {
        val flags = Pattern.CASE_INSENSITIVE or Pattern.LITERAL or Pattern.UNICODE_CASE
        return Pattern.compile(containsString, flags)
    }

    /**
     * Whether any of the patterns matches the given value.
     */
    fun findAny(patterns: Array<Pattern>, value: CharSequence): Boolean {
        return patterns.any { it.matcher(value).find() }
    }

    /**
     * Encodes the given string into a UTF-8 byte array.
     */
    fun utf8(value: String): ByteArray {
        return value.toByteArray(UTF_8)
    }

    /** Checks if the given value is null or empty. */
    fun isNullOrEmpty(value: String?): Boolean {
        return value.isNullOrEmpty()
    }

    /**
     * Converts the given bytes to a hexadecimal string.
     * @param bytes bytes to read.
     * @return String with hexadecimal values.
     */
    fun bytesToHex(bytes: ByteArray): String {
        val hexChars = CharArray(bytes.size * 2)
        for (i in bytes.indices) {
            val value = bytes[i].toInt() and 0xFF
            hexChars[i * 2] = HEX_ARRAY[value ushr 4]
            hexChars[i * 2 + 1] = HEX_ARRAY[value and 0x0F]
        }
        return String(hexChars)
    }
}
