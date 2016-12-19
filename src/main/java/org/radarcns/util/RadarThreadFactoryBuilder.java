package org.radarcns.util;

import javax.annotation.Nonnull;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Francesco Nobilia on 07/10/2016.
 */
public class RadarThreadFactoryBuilder {

    private String namePrefix = null;
    private boolean daemon = false;
    private int priority = Thread.NORM_PRIORITY;

    public RadarThreadFactoryBuilder setNamePrefix(@Nonnull String namePrefix) {
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
