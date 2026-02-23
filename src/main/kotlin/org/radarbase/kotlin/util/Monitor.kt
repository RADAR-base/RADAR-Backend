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

package org.radarbase.kotlin.util

import org.slf4j.Logger
import java.util.concurrent.atomic.AtomicInteger

/**
 * Monitors a count and buffer variable by printing out their values and resetting them.
 */
class Monitor(private val log: Logger, private val message: String) : Runnable {
    private val count = AtomicInteger(0)

    init {
        requireNotNull(log) { "Argument log may not be null" }
    }

    /**
     * Logs the current count and, if applicable buffer size. This resets the current count to 0.
     */
    override fun run() {
        log.info("{} {}", count.getAndSet(0), message)
    }

    /** Increment the count by one. */
    fun increment() {
        count.incrementAndGet()
    }
}
