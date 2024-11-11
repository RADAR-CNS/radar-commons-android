package org.radarbase.android.util

import java.io.IOException

interface Parser<S, T> {
    @Throws(IOException::class)
    suspend fun parse(value: S): T
}
