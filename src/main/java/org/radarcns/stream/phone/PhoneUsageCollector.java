package org.radarcns.stream.phone;

import org.radarcns.passive.phone.PhoneUsageEvent;
import org.radarcns.passive.phone.UsageEventType;

import java.math.BigDecimal;
import java.math.MathContext;

/**
 * Created by piotrzakrzewski on 27/07/2017.
 */
public class PhoneUsageCollector {
    private BigDecimal totalForegroundTime; // total time in seconds
    private double lastForegroundEvent; // date in Unix time in seconds
    private int timesTurnedOn;
    private String categoryName; // optional
    private Double categoryNameFetchTime; // optional

    public PhoneUsageCollector() {
        totalForegroundTime = BigDecimal.ZERO;
    }

    public PhoneUsageCollector update(PhoneUsageEvent event) {
        if (event.getCategoryName() != null) {
            this.categoryName = event.getCategoryName();
            this.categoryNameFetchTime = event.getCategoryNameFetchTime();
        }

        if (event.getEventType() == UsageEventType.FOREGROUND) {
            // Foreground event received
            timesTurnedOn++;
            lastForegroundEvent = event.getTime();
        } else if (event.getEventType() == UsageEventType.BACKGROUND
                && lastForegroundEvent != 0.0) {
            // Background event received for an app which was previously on.
            totalForegroundTime = totalForegroundTime.add(
                    BigDecimal.valueOf(event.getTime())
                            .subtract(BigDecimal.valueOf(lastForegroundEvent),
                                    MathContext.DECIMAL128));
            lastForegroundEvent = 0.0;
        }
        // else if eventType is background and it was already in the background, ignore.
        // We must have missed an event.
        // else if eventType is something else: ignore.

        return this;
    }

    public double getTotalForegroundTime() {
        return totalForegroundTime.doubleValue();
    }

    public void setTotalForegroundTime(double totalForegroundTime) {
        this.totalForegroundTime = BigDecimal.valueOf(totalForegroundTime);
    }

    public double getLastForegroundEvent() {
        return lastForegroundEvent;
    }

    public void setLastForegroundEvent(double lastForegroundEvent) {
        this.lastForegroundEvent = lastForegroundEvent;
    }

    public int getTimesTurnedOn() {
        return timesTurnedOn;
    }

    public void setTimesTurnedOn(int timesTurnedOn) {
        this.timesTurnedOn = timesTurnedOn;
    }

    public String getCategoryName() {
        return categoryName;
    }

    public void setCategoryName(String categoryName) {
        this.categoryName = categoryName;
    }

    public Double getCategoryNameFetchTime() {
        return categoryNameFetchTime;
    }

    public void setCategoryNameFetchTime(Double categoryNameFetchTime) {
        this.categoryNameFetchTime = categoryNameFetchTime;
    }
}
