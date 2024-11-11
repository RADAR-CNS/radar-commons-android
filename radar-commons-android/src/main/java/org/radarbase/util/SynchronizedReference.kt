package org.radarbase.util

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.io.IOException
import java.lang.ref.WeakReference

class SynchronizedReference<T>(private val supplier: suspend () -> T) {
    private val mutex = Mutex()
    private var ref: WeakReference<T>? = null

    @Throws(IOException::class)
    suspend fun get(): T = mutex.withLock {
        ref?.get()
            ?: supplier().also { ref = WeakReference(it) }
    }
}
