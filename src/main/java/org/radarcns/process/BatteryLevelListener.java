package org.radarcns.process;

import org.radarcns.Device;

/**
 * Created by joris on 27/09/2016.
 */
public interface BatteryLevelListener {
    enum Status {
        NORMAL, LOW, CRITICAL, EMPTY;
    }
    void batteryLevelStatusUpdated(Device device, Status status);
}
