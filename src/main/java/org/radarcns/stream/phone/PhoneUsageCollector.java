package org.radarcns.stream.phone;

import org.radarcns.phone.UsageEventType;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by piotrzakrzewski on 27/07/2017.
 */
public class PhoneUsageCollector {

    private Map<String, Double> totalForegroundTime = new HashMap<>(); // total time in seconds
    private Map<String, Double> lastForegroundEvent = new HashMap<>(); // date in Unix time in seconds
    private Map<String, Boolean> inTheForeground = new HashMap<>();
    private Map<String, Integer> timesTurnedOn = new HashMap<>();

    public PhoneUsageCollector update(UsageEventType eventType, double time, String packageName) {
        if (UsageEventType.FOREGROUND.equals(eventType)) {
            turnOn(packageName);
            if(!wasAlreadyOn(packageName)) {
                lastForegroundEvent.put(packageName, time);
            }
        } else if (UsageEventType.BACKGROUND.equals(eventType)) {
            if(wasAlreadyOn(packageName)) {
                incrementTurnedOn(packageName);
                updateUsageTime(packageName, time);
            }
            turnOff(packageName);
        }
        return this;
    }


    private void incrementTurnedOn(String packageName) {
        if (!timesTurnedOn.containsKey(packageName)) {
            timesTurnedOn.put(packageName, 0);
        }
        int current = timesTurnedOn.get(packageName);
        timesTurnedOn.put(packageName, current + 1);
    }

    private void turnOff(String packageName) {
        if (!inTheForeground.containsKey(packageName)) {
            inTheForeground.put(packageName, false);
        }
        inTheForeground.put(packageName, false);
    }

    private void turnOn(String packageName) {
        if (!inTheForeground.containsKey(packageName)) {
            inTheForeground.put(packageName, true);
        }
        inTheForeground.put(packageName, true);
    }

    private boolean wasAlreadyOn(String packageName) {
        if (!inTheForeground.containsKey(packageName)) {
            return false;
        } else {
            return inTheForeground.get(packageName);
        }
    }

    private void updateUsageTime(String packageName, double turnedOfTime) {
        if (!totalForegroundTime.containsKey(packageName)) {
            totalForegroundTime.put(packageName, 0d);
        }
        double lastUpdate = lastForegroundEvent.getOrDefault(packageName, null);
        double newIncrement = turnedOfTime - lastUpdate;
        double current = totalForegroundTime.get(packageName);
        totalForegroundTime.put(packageName, current + newIncrement);
    }

    public Map<String, Double> getTotalForegroundTime() {
        return totalForegroundTime;
    }

    public Map<String, Boolean> getInTheForeground() {
        return inTheForeground;
    }

    public Map<String, Integer> getTimesTurnedOn() {
        return timesTurnedOn;
    }
}
