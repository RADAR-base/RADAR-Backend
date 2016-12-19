package org.radarcns.process;

import org.radarcns.key.MeasurementKey;

public interface BatteryLevelListener {
    enum Status {
        NORMAL, LOW, CRITICAL, EMPTY
    }
    void batteryLevelStatusUpdated(MeasurementKey device, Status status);
}
