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

package org.radarcns.util;

import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;

/**
 * Monitors a count and buffer variable by printing out their values and resetting them.
 */
public class Monitor implements Runnable {
    private final AtomicInteger count;
    private final Logger log;
    private final String message;

    /**
     * Monitor a counter. Log messages with the count and the given message are sent to given
     * logger, separated by a space.
     * @param log logger to log messages to
     * @param message message to append to the current count
     */
    public Monitor(Logger log, String message) {
        if (log == null) {
            throw new IllegalArgumentException("Argument log may not be null");
        }
        this.count = new AtomicInteger(0);
        this.log = log;
        this.message = message;
    }

    /**
     * Logs the current count and, if applicable buffer size. This resets the current count to 0.
     */
    @Override
    public void run() {
        log.info("{} {}", count.getAndSet(0), message);
    }

    /** Increment the count by one. */
    public void increment() {
        this.count.incrementAndGet();
    }
}
