package org.radarcns.process;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatteryLevelLogger implements BatteryLevelListener {
    private final static Logger logger = LoggerFactory.getLogger(BatteryLevelLogger.class);

    @Override
    public void batteryLevelStatusUpdated(Device device, Status status) {
        switch (status) {
            case NORMAL:
                break;
            case LOW:
                logger.warn("Battery level of device {} is low", device.getId());
                break;
            case CRITICAL:
                logger.warn("Battery level of device {} is critically low", device.getId());
                break;
            case EMPTY:
                logger.error("Battery of device {} is empty", device.getId());
                break;
        }
    }
}
