package org.radarcns.stream.phone;

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

    public PhoneUsageCollector update(UsageEventType eventType, double time, String packageName) {
        this.packageName = packageName;
        if (eventType.equals(UsageEventType.FOREGROUND) && !inTheForeground) {
            // Foreground event received and was not in the foreground already
            // I am not sure if this is even possible, but will not hurt
            timesTurnedOn++;
            inTheForeground = true;
            lastForegroundEvent = time;
        } else if (eventType.equals(UsageEventType.BACKGROUND) && inTheForeground) {
            // Background event received for an app which was previously on.
            inTheForeground = false;
            updateUsageTime(time);
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

    public double getLastForegroundEvent() {
        return lastForegroundEvent;
    }

    public boolean isInTheForeground() {
        return inTheForeground;
    }

    public int getTimesTurnedOn() {
        return timesTurnedOn;
    }

    public String getPackageName() {
        return packageName;
    }
}
