package org.radarbase.android.util

import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicReference

class DelayedRetry(
    private val minDelay: Long,
    private val maxDelay: Long,
) {
    private val startValue = CurrentDelay(minDelay, (minDelay / 2).coerceAtLeast(1L))

    private val _currentDelay: AtomicReference<CurrentDelay> = AtomicReference(startValue)

    val currentDelay: Long
        get() = _currentDelay.get().value

    fun nextDelay(): Long {
        val current = _currentDelay.get()
        val nextMax = (2 * current.max).coerceAtMost(maxDelay)
        val nextValue = ThreadLocalRandom.current().nextLong(minDelay, nextMax)
        val nextDelay = CurrentDelay(nextValue, nextMax)
        return if (_currentDelay.compareAndSet(current, nextDelay)) {
            nextValue
        } else {
            _currentDelay.get().value
        }
    }

    fun reset() {
        _currentDelay.set(startValue)
    }

    data class CurrentDelay(val value: Long, val max: Long)
}
