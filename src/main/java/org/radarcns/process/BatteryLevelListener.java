package org.radarcns.process;

import org.radarcns.Device;

public interface BatteryLevelListener {
    enum Status {
        NORMAL, LOW, CRITICAL, EMPTY;
    }
    void batteryLevelStatusUpdated(Device device, Status status);
}
