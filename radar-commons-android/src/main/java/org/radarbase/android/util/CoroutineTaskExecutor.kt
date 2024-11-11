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

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.ExecutionException
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

/**
 * Coroutine-based task executor that provides a flexible API for managing, scheduling,
 * and safely executing tasks in a concurrent environment. Supports options for delayed
 * and repeated task execution, exception handling, cancellation, and synchronization.
 * Designed as a coroutine-based replacement for traditional handlers with additional
 * features for coroutine-safe task management.
 *
 * @property invokingClassName Name of the invoking class, used for logging purposes.
 * @property coroutineDispatcher Dispatcher for executing tasks within this executor.
 */
class CoroutineTaskExecutor(
    private val invokingClassName: String,
    private val coroutineDispatcher: CoroutineDispatcher = Dispatchers.Default,
) {
    private var job: Job? = null
    private val executorExceptionHandler: CoroutineExceptionHandler =
        CoroutineExceptionHandler { _, ex ->
            logger.error("Failed to run task", ex)
            throw ex
        }

    private var executorScope: CoroutineScope? = null

    private val nullValue: Any = Any()
    val isStarted: AtomicBoolean = AtomicBoolean(executorScope != null)
    private val executeMutex: Mutex = Mutex(false)
    private val jobExecuteMutex: Mutex = Mutex(false)
    private val computeMutex: Mutex = Mutex(false)

    private val activeTasks: ConcurrentLinkedQueue<CoroutineFutureExecutor> =
        ConcurrentLinkedQueue<CoroutineFutureExecutor>()

    /**
     * Starts the executor, initializing the coroutine scope with a specified [Job],
     * [coroutineDispatcher], and an exception handler.
     *
     * @param job The root job for this executor's coroutine scope. Defaults to a new [Job].
     */
    fun start(job: Job = Job())  {
        if (isStarted.get()) {
            logger.warn("Tried to start executor multiple times.")
            return
        }
        this.job = job
        executorScope = CoroutineScope(coroutineDispatcher + job + executorExceptionHandler)
        isStarted.set(true)
    }
    /**
     * Executes a suspendable task and suspends until it completes. If an exception occurs,
     * it throws an [ExecutionException].
     *
     * @param work The suspendable task to be executed.
     * @throws ExecutionException if the task encounters an error during execution.
     */
    @Throws(ExecutionException::class)
    suspend fun waitAndExecute(work: suspend () -> Unit): Unit = computeResult(work)!!

    /**
     * Executes a suspendable task and returns its result. If an exception occurs,
     * it throws an [ExecutionException].
     *
     * @param work The suspendable task to be executed.
     * @param T The return type of the task.
     * @return The result of the task, or null if it returns null.
     * @throws ExecutionException if the task encounters an error during execution.
     */
    @Throws(ExecutionException::class)
    suspend fun <T> computeResult(work: suspend () -> T?): T? = suspendCoroutine { continuation ->
        val activeStatus: Boolean? = executorScope?.isActive
        if (activeStatus == null || activeStatus == false) {
            logger.warn("Scope is already cancelled")
            continuation.resume(null)
            return@suspendCoroutine
        }
        checkExecutorStarted() ?: return@suspendCoroutine
        executorScope?.launch {
            try {
                val result = work() ?: nullValue
                if (result == nullValue) {
                    continuation.resume(null)
                } else {
                    @Suppress("UNCHECKED_CAST")
                    continuation.resume(result as T)
                }
            } catch (ex: Exception) {
                continuation.resumeWithException(ExecutionException(ex))
            }
        }
    }

    /**
     * Executes a task and returns its non-null result in a blocking manner, making it suitable for non-suspending calls.
     * If the executor scope is inactive or not started, logs a warning and returns null.
     *
     * @param work The suspendable task to be executed.
     * @param T The return type of the task.
     * @return The non-null result of the task.
     * @throws ExecutionException if the task fails during execution.
     * @throws IllegalStateException if the result is null or if the executor scope is inactive.
     */
    @Throws(ExecutionException::class, IllegalStateException::class)
    fun <T : Any> nonSuspendingCompute(work: suspend () -> T): T {
        return try {
            runBlocking(Dispatchers.Default) {
                computeMutex.withLock {
                    async {
                        try {
                            work()
                        } catch (ex: Exception) {
                            throw ExecutionException(ex)
                        }
                    }.await()
                }
            }
        } catch (ex: ExecutionException) {
            logger.error("Failed to execute work in nonSuspendingCompute", ex)
            throw ex
        }
    }

    /**
     * This method serves as an alias for [execute] to indicate that the task allows reentrancy.
     *
     * @param task The suspendable task to be executed.
     */
    fun executeReentrant(task: suspend () -> Unit) = execute(task)

    /**
     * Executes a task within a coroutine while ensuring that multiple tasks are synchronized
     * using a mutex, preventing concurrent access.
     *
     * @param task The suspendable task to be executed.
     */
    fun execute(task: suspend () -> Unit) {
        val activeStatus: Boolean? = executorScope?.isActive
        if (activeStatus == null || activeStatus == false) {
            logger.warn("Can't execute task, scope is already cancelled")
            return
        }
        checkExecutorStarted() ?: return

        executorScope?.launch {
            runTaskSafely(task)
        }
    }

    /**
     * Executes a task and returns its job instance. Useful for advanced task management
     * and monitoring.
     *
     * @param task The suspendable task to be executed.
     * @return A [Job] representing the task, or null if execution fails.
     */
    fun returnJobAndExecute(task: suspend () -> Unit): Job? {
        val activeStatus: Boolean? = executorScope?.isActive
        if (activeStatus == null || activeStatus == false) {
            logger.warn("Can't execute task and return job, scope is already cancelled")
            return null
        }
        checkExecutorStarted() ?: return null
        return executorScope?.launch {
            jobExecuteMutex.withLock {
                runTaskSafely(task)
            }
        }
    }

    /**
     * Schedules a task to run after a specified delay.
     *
     * @param delayMillis Delay in milliseconds before executing the task.
     * @param task The task to execute after the delay.
     * @return A [CoroutineFutureHandle] to manage the delayed task.
     */
    fun delay(
        delayMillis: Long,
        task: suspend () -> Unit
    ): CoroutineFutureHandle? {
        checkExecutorStarted() ?: return null
        val future = CoroutineFutureExecutorRef()
        activeTasks.add(future)
        future.startDelayed(delayMillis, task)
        return CoroutineFutureHandleRef(future.job, task)
    }

    /**
     * Repeatedly executes a task with a specified delay between executions, until canceled.
     *
     * @param delayMillis Delay in milliseconds between each execution.
     * @param task The task to execute repeatedly.
     * @return A [CoroutineFutureHandle] to manage the repeated task.
     */
    fun repeat(
        delayMillis: Long,
        task: suspend () -> Unit
    ): CoroutineFutureHandle? {
        checkExecutorStarted() ?: return null
        val future = CoroutineFutureExecutorRef()
        activeTasks.add(future)
        future.startRepeated(delayMillis, task)
        return CoroutineFutureHandleRef(future.job, task)
    }

    /**
     * Cancels all scheduled and active tasks.
     */
    private fun cancelAllFutures() {
        activeTasks.forEach { it.cancel() }
        activeTasks.clear()
    }

    /**
     * Stops the executor and cancels all active and scheduled tasks. Optionally runs
     * a finalization task before complete cancellation.
     *
     * @param finalization A finalization task to run before stopping the executor.
     */
    fun stop(finalization: (suspend () -> Unit)? = null) {
        finalization?.let {
            executorScope?.also { execScope ->
                checkExecutorStarted() ?: return@let
                runBlocking(coroutineDispatcher) {
                    joinAll(execScope.launch {
                        runTaskSafely(it)
                        logger.info("Executed the finalization task")
                    })
                }
            } ?: runBlocking(coroutineDispatcher) {
                runTaskSafely(it)
            }
        }
        cancelAllFutures()
        job?.cancel()
        isStarted.set(false)
        job = null
        executorScope = null
    }


    /**
     * Safely runs a task, logging errors if execution fails.
     *
     * @param task The suspendable task to be executed.
     */
    private suspend fun runTaskSafely(task: suspend () -> Unit) {
        val executionResult = runCatching {
            task()
        }
        if (executionResult.isFailure) {
            logger.error("Failed to execute task: {}", executionResult.toString())
        }
    }

    /**
     * Interface representing a managed task executor with methods for delay, repetition,
     * and cancellation.
     */
    interface CoroutineFutureExecutor {

        fun startDelayed(delayMillis: Long, task: suspend () -> Unit)

        fun startRepeated(delayMillis: Long, task: suspend () -> Unit)

        fun startRepeatedUntilFalse(
            delayMillis: Long,
            task: suspend () -> Boolean
        )

        fun cancel()
    }

    /**
     * Interface for handling task management with options for awaiting, running, and
     * canceling the task.
     */
    interface CoroutineFutureHandle {
        suspend fun awaitNow()

        suspend fun runNow()

        fun cancel()
    }

    /**
     * Implementation of [CoroutineFutureHandle] that provides locking and job management
     * features for the associated task.
     */
    inner class CoroutineFutureHandleRef(
        private var job: Job?,
        private val task: suspend () -> Unit
    ) : CoroutineFutureHandle {

        private val handleMutex: Mutex = Mutex()

        override suspend fun awaitNow() {
            handleMutex.withLock {
                job?.cancel()
                waitAndExecute(task)
            }
        }

        override suspend fun runNow() {
            handleMutex.withLock {
                job?.cancel()
                execute(task)
            }
        }

        @Synchronized
        override fun cancel() {
            job?.also {
                it.cancel()
                job = null
            }
        }
    }

    /**
     * Internal class managing delayed and repeated task execution, with cancellation
     * capabilities.
     */
    inner class CoroutineFutureExecutorRef : CoroutineFutureExecutor {

        var job: Job? = null

        override fun startDelayed(delayMillis: Long, task: suspend () -> Unit) {
            checkNullJob() ?: return
            job = executorScope?.launch {
                delay(delayMillis)
                runTaskSafely(task)
            }
        }

        override fun startRepeated(delayMillis: Long, task: suspend () -> Unit) {
            checkNullJob() ?: return
            job = executorScope?.launch {
                while (isActive) {
                    delay(delayMillis)
                    runTaskSafely(task)
                }
            }
        }

        override fun startRepeatedUntilFalse(
            delayMillis: Long,
            task: suspend () -> Boolean
        ) {
            checkNullJob() ?: return
            job = executorScope?.launch {
                var lastResult = true
                while (isActive && lastResult) {
                    delay(delayMillis)
                    lastResult = try {
                        task()
                    } catch (ex: Exception) {
                        false
                    }
                }
            }
        }

        private fun checkNullJob(): Unit? {
            if (job != null) {
                logger.warn("Tried re-assigning the already assigned job")
                return null
            }
            return Unit
        }

        override fun cancel() {
            job?.also {
                it.cancel()
                job = null
            }
        }
    }

    /**
     * Checks if the executor has been started; logs a warning if not.
     *
     * @return True if the executor is started, otherwise null.
     */
    private fun checkExecutorStarted(): Boolean? {
        if (!isStarted.get()) {
            logger.warn("Either executor not started yet or it has already stopped! Please call start() to execute tasks")
        }
        return if (isStarted.get()) return true else null
    }

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(CoroutineTaskExecutor::class.java)
    }
}