/*
 * Copyright 2017 King's College London and The Hyve
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

package org.radarbase.monitor

import org.slf4j.LoggerFactory
import java.io.IOException
import java.time.Duration
import java.util.*
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.stream.Collectors
import java.util.stream.Stream

/**
 * Runs multiple monitors, each in its own thread.
 */
class CombinedKafkaMonitor(monitors: Stream<KafkaMonitor>) : KafkaMonitor {
    private val monitors: List<KafkaMonitor> =
        Objects.requireNonNull(monitors).filter(Objects::nonNull).collect(Collectors.toList())

    private val done = AtomicBoolean(false)
    private var executor: ExecutorService? = null
    private var ioException: IOException? = null
    private var interruptedException: InterruptedException? = null

    init {
        require(this.monitors.isNotEmpty()) { "Monitor collection may not be empty" }
    }

    override val isShutdown: Boolean
        get() = done.get()

    override var pollTimeout: Duration
        get() = monitors[0].pollTimeout
        set(pollTimeout) {
            for (monitor in monitors) {
                monitor.pollTimeout = pollTimeout
            }
        }

    override fun start() {
        synchronized(this) {
            if (executor != null) {
                throw IllegalStateException("Cannot start monitor twice")
            }
            executor = Executors.newFixedThreadPool(monitors.size)
        }

        val currentExecutor = executor!!
        for (monitor in monitors) {
            currentExecutor.submit {
                try {
                    monitor.start()
                } catch (ex: IOException) {
                    setIoException(ex)
                } catch (ex: InterruptedException) {
                    setInterruptedException(ex)
                }
            }
        }

        currentExecutor.shutdown()
        currentExecutor.awaitTermination(366_000, TimeUnit.DAYS) // > 1000 years...

        getIoException()?.let { throw it }
        getInterruptedException()?.let { throw it }
    }

    @Synchronized
    private fun getIoException(): IOException? = ioException

    @Synchronized
    private fun setIoException(ioException: IOException) {
        this.ioException = ioException
        initiateShutdownIgnoreException()
    }

    @Synchronized
    private fun getInterruptedException(): InterruptedException? = interruptedException

    @Synchronized
    private fun setInterruptedException(interruptedException: InterruptedException) {
        this.interruptedException = interruptedException
        initiateShutdownIgnoreException()
    }

    private fun initiateShutdownIgnoreException() {
        try {
            initiateShutdown()
        } catch (ex: InterruptedException) {
            logger.info("Ignoring additional InterruptedException", ex)
        } catch (ex: IOException) {
            logger.info("Ignoring additional IOException", ex)
        }
    }

    private fun initiateShutdown() {
        if (!done.getAndSet(true)) {
            for (monitor in monitors) {
                try {
                    monitor.shutdown()
                } catch (ex: Exception) {
                    logger.warn("Failed to shutdown monitor ${monitor.javaClass.simpleName}", ex)
                }
            }
        }
    }

    override fun shutdown() {
        synchronized(this) {
            if (executor == null) {
                return
            }
        }
        initiateShutdown()
        executor?.let {
            it.awaitTermination(30, TimeUnit.SECONDS)
            it.shutdownNow()
        }
    }

    fun getMonitors(): List<KafkaMonitor> = ArrayList(monitors)

    companion object {
        private val logger = LoggerFactory.getLogger(CombinedKafkaMonitor::class.java)
    }
}
