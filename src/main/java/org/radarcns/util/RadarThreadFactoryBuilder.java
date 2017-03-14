/*
 * Copyright 2017 Kings College London and The Hyve
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

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnull;

/**
 * Created by Francesco Nobilia on 07/10/2016.
 */
public class RadarThreadFactoryBuilder {

    private String namePrefix = null;
    private boolean daemon = false;
    private int priority = Thread.NORM_PRIORITY;

    public RadarThreadFactoryBuilder setNamePrefix(@Nonnull String namePrefix) {
        if (namePrefix == null) {
            throw new IllegalArgumentException("namePrefix cannot be null");
        }
        this.namePrefix = namePrefix;
        return this;
    }

    public RadarThreadFactoryBuilder setDaemon(boolean daemon) {
        this.daemon = daemon;
        return this;
    }

    public RadarThreadFactoryBuilder setPriority(int priority) {
        if (priority < Thread.MIN_PRIORITY) {
            throw new IllegalArgumentException("Thread priority " + priority
                    + " must be >= " + Thread.MIN_PRIORITY);
        }

        if (priority > Thread.MAX_PRIORITY) {
            throw new IllegalArgumentException("Thread priority " + priority
                    + " must be <= " + Thread.MAX_PRIORITY);
        }

        this.priority = priority;
        return this;
    }

    public ThreadFactory build() {
        return build(this);
    }

    private static ThreadFactory build(RadarThreadFactoryBuilder builder) {
        final String namePrefix = builder.namePrefix;
        final boolean daemon = builder.daemon;
        final int priority = builder.priority;

        final AtomicLong count = new AtomicLong(0);

        return runnable -> {
            Thread thread = new Thread(runnable);
            if (namePrefix != null) {
                thread.setName(namePrefix + "-" + count.getAndIncrement());
            }
            thread.setDaemon(daemon);
            thread.setPriority(priority);

            return thread;
        };
    }
}
