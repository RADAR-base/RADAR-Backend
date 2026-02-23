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

import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicLong

/**
 * Created by Francesco Nobilia on 07/10/2016.
 */
class RadarThreadFactoryBuilder {
    private var namePrefix: String? = null
    private var isDaemon = false
    private var priority = Thread.NORM_PRIORITY

    fun setNamePrefix(namePrefix: String): RadarThreadFactoryBuilder {
        requireNotNull(namePrefix) { "namePrefix cannot be null" }
        this.namePrefix = namePrefix
        return this
    }

    fun setDaemon(daemon: Boolean): RadarThreadFactoryBuilder {
        this.isDaemon = daemon
        return this
    }

    fun setPriority(priority: Int): RadarThreadFactoryBuilder {
        require(priority >= Thread.MIN_PRIORITY) {
            "Thread priority $priority must be >= ${Thread.MIN_PRIORITY}"
        }
        require(priority <= Thread.MAX_PRIORITY) {
            "Thread priority $priority must be <= ${Thread.MAX_PRIORITY}"
        }
        this.priority = priority
        return this
    }

    fun build(): ThreadFactory {
        val namePrefix = this.namePrefix
        val isDaemon = this.isDaemon
        val priority = this.priority
        val count = AtomicLong(0)

        return ThreadFactory { runnable ->
            Thread(runnable).apply {
                if (namePrefix != null) {
                    name = "$namePrefix-${count.getAndIncrement()}"
                }
                setDaemon(isDaemon)
                this.priority = priority
            }
        }
    }
}
