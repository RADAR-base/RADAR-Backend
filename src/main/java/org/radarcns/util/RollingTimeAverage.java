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

import java.util.Deque;
import java.util.LinkedList;

/**
 * Get the average of a set of values collected in a sliding time window of fixed duration.
 *
 * <p>At least one value is needed to get an average.
 */
public class RollingTimeAverage {
    private final long window;
    private final Deque<TimeCount> deque;
    private TimeCount firstTime;
    private double total;

    /**
     * A rolling time average with a sliding time window of fixed duration.
     * @param timeWindowMillis duration of the time window.
     */
    public RollingTimeAverage(long timeWindowMillis) {
        this.window = timeWindowMillis;
        this.total = 0d;
        this.firstTime = null;
        this.deque = new LinkedList<>();
    }

    /** Whether values have already been added. */
    public boolean hasAverage() {
        return firstTime != null;
    }

    /** Add a new value. */
    public void add(double x) {
        if (firstTime == null) {
            firstTime = TimeCount.create(x);
        } else {
            deque.addLast(TimeCount.create(x));
        }
        total += x;
    }

    /**
     * Get the average value per second over a sliding time window of fixed size.
     *
     * <p>It takes one value before the window started as a baseline, and adds all values in the
     * window. It then divides by the total time window from the first value (outside/before the
     * window) to the last value (at the end of the window).
     * @return average value per second
     */
    public double getAverage() {
        if (!hasAverage()) {
            throw new IllegalStateException("Cannot get average without values");
        }

        long now = System.currentTimeMillis();
        long currentWindowStart = now - window;

        while (!deque.isEmpty() && deque.getFirst().time < currentWindowStart) {
            total -= firstTime.value;
            firstTime = deque.removeFirst();
        }
        double timeWindow;
        double valueOutsideWindow;
        if (deque.isEmpty() || firstTime.time >= currentWindowStart) {
            timeWindow = now - firstTime.time;
            valueOutsideWindow = 0d;
        } else {
            timeWindow = deque.getLast().time - currentWindowStart;
            double firstPartRatio = (currentWindowStart - firstTime.time)
                    / (deque.getFirst().time - firstTime.time);
            valueOutsideWindow = firstTime.value + deque.getFirst().value * firstPartRatio;
        }
        return 1000d * (total - valueOutsideWindow) / timeWindow;
    }

    private static class TimeCount {
        private final double value;
        private final long time;

        private TimeCount(long time, double value) {
            this.value = value;
            this.time = time;
        }

        private static TimeCount create(double value) {
            return new TimeCount(System.currentTimeMillis(), value);
        }
    }
}
