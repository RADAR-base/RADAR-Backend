package org.radarcns.util;

import java.util.Deque;
import java.util.LinkedList;

public class RollingTimeAverage {
    private final long window;
    private TimeCount firstTime;
    private double total;
    private Deque<TimeCount> deque;

    public RollingTimeAverage(long timeWindowMillis) {
        this.window = timeWindowMillis;
        this.total = 0d;
        this.firstTime = null;
        this.deque = new LinkedList<>();
    }

    public void add(double x) {
        if (firstTime == null) {
            firstTime = new TimeCount(x);
        } else {
            deque.addLast(new TimeCount(x));
        }
        total += x;
    }

    public double getAverage() {
        long now = System.currentTimeMillis();
        long currentWindowStart = now - window;

        if (this.firstTime == null) {
            throw new IllegalStateException("Cannot get average without values");
        }

        while (!this.deque.isEmpty() && this.deque.getFirst().time < currentWindowStart) {
            total -= this.firstTime.value;
            this.firstTime = this.deque.removeFirst();
        }
        return 1000d * total / (now - this.firstTime.time);
    }

    static class TimeCount {
        private final double value;
        private final long time;

        TimeCount(double value) {
            this.value = value;
            this.time = System.currentTimeMillis();
        }
    }
}
