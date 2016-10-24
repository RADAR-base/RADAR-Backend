package org.radarcns.process;

public interface BatteryLevelListener {
    enum Status {
        NORMAL, LOW, CRITICAL, EMPTY;
    }
    void batteryLevelStatusUpdated(Device device, Status status);
}
