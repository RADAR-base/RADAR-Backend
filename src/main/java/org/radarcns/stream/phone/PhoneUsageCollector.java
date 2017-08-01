package org.radarcns.stream.phone;

import org.radarcns.phone.PhoneUsageEvent;
import org.radarcns.phone.UsageEventType;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by piotrzakrzewski on 27/07/2017.
 */
public class PhoneUsageCollector {

    private double totalForegroundTime ; // total time in seconds
    private double lastForegroundEvent ; // date in Unix time in seconds
    private boolean inTheForeground;
    private int timesTurnedOn;
    private String packageName;
    private String categoryName;
    private double categoryNameFetchTime;

    public PhoneUsageCollector update(
            PhoneUsageEvent event) {
        this.packageName = event.getPackageName();
        this.categoryName = event.getCategoryName();
        this.categoryNameFetchTime = event.getCategoryNameFetchTime();
        double time = event.getTime();
        if (event.getEventType().equals(UsageEventType.FOREGROUND) && !inTheForeground) {
            // Foreground event received and was not in the foreground already
            // I am not sure if this is even possible, but will not hurt
            timesTurnedOn++;
            inTheForeground = true;
            lastForegroundEvent = time;
        } else if (event.getEventType().equals(UsageEventType.FOREGROUND) && inTheForeground) {
            lastForegroundEvent = time; // We assume that a background event was missed. Time is reset.
        }
        else if (event.getEventType().equals(UsageEventType.BACKGROUND) && inTheForeground) {
            // Background event received for an app which was previously on.
            inTheForeground = false;
            updateUsageTime(time);
        } else if (event.getEventType().equals(UsageEventType.BACKGROUND) && !inTheForeground) {
            // TODO: do we need to handle this case in any way?
        }

        return this;
    }


    private void updateUsageTime( double turnedOfTime) {
        double newIncrement = turnedOfTime - lastForegroundEvent;
        totalForegroundTime +=  newIncrement;
    }

    public double getTotalForegroundTime() {
        return totalForegroundTime;
    }

    public void setTotalForegroundTime(double totalForegroundTime) {
        this.totalForegroundTime = totalForegroundTime;
    }

    public double getLastForegroundEvent() {
        return lastForegroundEvent;
    }

    public void setLastForegroundEvent(double lastForegroundEvent) {
        this.lastForegroundEvent = lastForegroundEvent;
    }

    public boolean isInTheForeground() {
        return inTheForeground;
    }

    public void setInTheForeground(boolean inTheForeground) {
        this.inTheForeground = inTheForeground;
    }

    public int getTimesTurnedOn() {
        return timesTurnedOn;
    }

    public void setTimesTurnedOn(int timesTurnedOn) {
        this.timesTurnedOn = timesTurnedOn;
    }

    public String getPackageName() {
        return packageName;
    }

    public void setPackageName(String packageName) {
        this.packageName = packageName;
    }

    public String getCategoryName() {
        return categoryName;
    }

    public void setCategoryName(String categoryName) {
        this.categoryName = categoryName;
    }

    public double getCategoryNameFetchTime() {
        return categoryNameFetchTime;
    }

    public void setCategoryNameFetchTime(double categoryNameFetchTime) {
        this.categoryNameFetchTime = categoryNameFetchTime;
    }
}
