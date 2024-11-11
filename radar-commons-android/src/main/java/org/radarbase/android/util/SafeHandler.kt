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

import android.os.Handler
import android.os.HandlerThread
import android.os.Looper
import androidx.annotation.Keep
import org.radarbase.android.util.SafeHandler.Companion.getInstance
import org.slf4j.LoggerFactory
import java.lang.ref.WeakReference
import java.util.concurrent.ExecutionException
import java.util.concurrent.SynchronousQueue

/**
 * A wrapper around Android Handler that makes some operations easier or safer in terms of exception
 * handling and multithreading.
 * @constructor consider using [getInstance] instead for shared or reinitializing handlers.
 */
@Suppress("unused", "MemberVisibilityCanBePrivate")
class SafeHandler(
    val name: String,
    private val priority: Int,
) {
    private var handlerThread: HandlerThread? = null

    /** Whether the handler has been started. */
    @get:Synchronized
    val isStarted: Boolean
        get() = handler != null

    /**
     * Direct access to the handler. Can be used for outside code that requires an actual
     * handler. Using this means that the handler should be stopped with great care, preferably
     * by ensuring during finalization in [stop] that the handler is not being used anymore.
     */
    @get:Synchronized
    var handler: Handler? = null
        private set

    @Synchronized
    fun start() {
        if (isStarted) {
            logger.warn("Tried to start SafeHandler multiple times.")
            return
        }

        handlerThread = HandlerThread(name, priority).apply {
            start()
            handler = Handler(looper)
        }
    }

    /**
     * Run a command in the given handler, waiting for it to finish.
     * @throws ExecutionException if any exception occurred in [runnable]
     */
    @Throws(InterruptedException::class, ExecutionException::class)
    fun await(runnable: Runnable) = compute { runnable.run() }

    /**
     * Run a command in the given handler, waiting for it to finish.
     * @throws ExecutionException if any exception occurred in [runnable]
     */
    @Throws(InterruptedException::class, ExecutionException::class)
    fun await(runnable: () -> Unit) = compute(runnable)

    /**
     * Compute a value the given handler, returning its value after it finishes.
     * @throws ExecutionException if any exception occurred in [method]
     */
    @Throws(InterruptedException::class, ExecutionException::class)
    fun <T> compute(method: () -> T): T {
        if (Thread.currentThread() == handlerThread) {
            try {
                return method()
            } catch (ex: Exception) {
                throw ExecutionException(ex)
            }
        } else {
            val queue = SynchronousQueue<Any>()
            execute {
                try {
                    queue.put(method() ?: nullMarker)
                } catch (ex: Exception) {
                    queue.put(ExecutionException(ex))
                }
            }
            val result = queue.take()
            @Suppress("UNCHECKED_CAST")
            return when {
                result === nullMarker -> null
                result is ExecutionException -> throw result
                else -> result
            } as T
        }
    }

    /**
     * Executes [runnable] on this handler. If the handler has already been stopped, this does not
     * get executed.
     */
    fun execute(runnable: Runnable) = execute(false, runnable::run)

    /**
     * Executes [runnable] on this handler. If the handler has already been stopped, this does not
     * get executed.
     */
    fun execute(runnable: () -> Unit) = execute(false, runnable)

    /**
     * Executes [runnable] on this handler. If this method called from the handler itself,
     * runnable is executed immediately, instead of putting it in the queue.
     */
    fun executeReentrant(runnable: () -> Unit) {
        if (Thread.currentThread() == handlerThread) {
            runnable.tryRunOrNull()
        } else {
            execute(runnable)
        }
    }

    /**
     * Executes [runnable] on this handler. If this method called from the handler itself,
     * runnable is executed immediately, instead of putting it in the queue.  If the handler has
     * been stopped and [defaultToCurrentThread] is true, runnable will still be executed on the
     * current thread. If the handler is stopped and [defaultToCurrentThread] is false, the runnable
     * will not be executed.
     */
    fun executeReentrant(defaultToCurrentThread: Boolean, runnable: () -> Unit) {
        if (Thread.currentThread() == handlerThread) {
            runnable.tryRunOrNull()
        } else {
            execute(defaultToCurrentThread, runnable)
        }
    }

    /**
     * Executes [runnable] on this handler. If this method called from the handler itself,
     * runnable is executed immediately, instead of putting it in the queue.
     */
    fun executeReentrant(runnable: Runnable) = executeReentrant(runnable::run)

    /**
     * Executes [runnable] on a handler. If the handler has been stopped and
     * [defaultToCurrentThread] is true, runnable will still be executed on the current thread.
     * If the handler is stopped and [defaultToCurrentThread] is false, the runnable will not be
     * executed.
     */
    fun execute(defaultToCurrentThread: Boolean, runnable: Runnable) = execute(defaultToCurrentThread, runnable::run)

    /**
     * Executes [runnable] on a handler. If the handler has been stopped and
     * [defaultToCurrentThread] is true, runnable will still be executed on the current thread.
     * If the handler is stopped and [defaultToCurrentThread] is false, the runnable will not be
     * executed.
     */
    fun execute(defaultToCurrentThread: Boolean, runnable: () -> Unit) {
        val didRun = synchronized(this) {
            handler?.post { runnable.tryRunOrNull() }
        } ?: false

        if (!didRun && defaultToCurrentThread) {
            runnable.tryRunOrNull()
        }
    }

    /**
     * Executes [runnable] on a handler after [delay] milliseconds.
     * @return reference to the task to be run, or null if the handler was stopped.
     */
    fun delay(delay: Long, runnable: Runnable): HandlerFuture? = delay(delay, runnable::run)

    /**
     * Executes [runnable] on a handler after [delay] milliseconds.
     */
    @Synchronized
    fun delay(delay: Long, runnable: () -> Unit): HandlerFuture? {
        val handler = handler ?: return null
        val r = Runnable {
            runnable.tryRunOrNull()
        }
        handler.postDelayed(r, delay)
        return HandlerFutureRef(r)
    }

    /**
     * Repeat given [runnable] after every [delay] milliseconds, as long as it returns true.
     */
    fun repeatWhile(delay: Long, runnable: () -> Boolean): HandlerFuture? = this.delay(delay) {
        if (runnable.tryRunOrNull() == true) repeatWhile(delay, runnable)
    }

    /**
     * Repeat given [runnable] after every [delay] milliseconds.
     */
    fun repeat(delay: Long, runnable: () -> Unit): HandlerFuture? = this.delay(delay) {
        runnable.tryRunOrNull()
        repeat(delay, runnable)
    }

    /**
     * Stop the handler, running [finalization] as the last operation. If the handler
     * was already stopped, the finalization is still run, but on the current thread.
     */
    fun stop(finalization: Runnable) = stop(finalization::run)

    @Synchronized
    fun interrupt() = handlerThread?.interrupt()

    /**
     * Stop the handler, running [finalization], if any, as the last operation. If the handler
     * was already stopped, the finalization is still run, but on the current thread. If the thread
     * was never started, none of the finalization is run.
     */
    @Synchronized
    fun stop(finalization: (() -> Unit)? = null, currentThreadFinalization: (() -> Unit)? = null) {
        val thread = handlerThread ?: return

        currentThreadFinalization?.tryRunOrNull()

        val oldHandler = handler
        if (oldHandler != null) {
            handler = null
            if (finalization != null) {
                oldHandler.post { finalization.tryRunOrNull() }
            }
        } else {
            finalization?.tryRunOrNull()
        }
        thread.quitSafely()

        handlerThread = null
    }

    /**
     * A Future to evaluate the result of a [delay] call.
     */
    @Keep
    interface HandlerFuture {
        /**
         * Do not wait for a delay, but execute the supplied runnable immediately and wait for it
         * to finish.
         */
        @Throws(InterruptedException::class, ExecutionException::class)
        fun awaitNow()

        /**
         * Do not wait for a delay, but execute the supplied runnable immediately. If this is called
         * within the handler, it will be called directly.
         */
        fun runNow()

        /**
         * Cancel running this runnable. If the runnable has already run or is running, this has no
         * effect.
         */
        fun cancel()
    }

    private inner class HandlerFutureRef(val runnable: Runnable): HandlerFuture {
        override fun awaitNow(): Unit = synchronized(this@SafeHandler) {
            handler?.removeCallbacks(runnable)
            await(runnable)
        }
        override fun runNow(): Unit = synchronized(this@SafeHandler) {
            handler?.removeCallbacks(runnable)
            executeReentrant(runnable)
        }
        override fun cancel(): Unit = synchronized(this@SafeHandler) {
            handler?.removeCallbacks(runnable)
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(SafeHandler::class.java)
        private val nullMarker = Any()
        private val map: MutableMap<String, WeakReference<SafeHandler>> = HashMap()

        /**
         * Get a shared instance of a handler. Note that for this to be safe, the app should
         * not stop the handler. This can be used for a client library that is very sensitive about
         * being moved to another thread.
         */
        @Synchronized
        fun getInstance(
            name: String,
            priority: Int,
        ): SafeHandler {
            val handlerRef = map[name]?.get()
            return handlerRef
                ?: run {
                    val handler = SafeHandler(name, priority)
                    map[name] = WeakReference(handler)
                    handler
                }
        }

        private fun <T> (() -> T).tryRunOrNull(): T? = try {
            this()
        } catch (ex: Exception) {
            logger.error("Failed to run posted runnable", ex)
            null
        }
    }
}
