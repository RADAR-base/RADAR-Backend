package org.radarcns.process;

import org.radarcns.key.MeasurementKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatteryLevelLogger implements BatteryLevelListener {
    private final static Logger logger = LoggerFactory.getLogger(BatteryLevelLogger.class);

    @Override
    public void batteryLevelStatusUpdated(MeasurementKey device, Status status) {
        switch (status) {
            case NORMAL:
                break;
            case LOW:
                logger.warn("Battery level of sensor {} of user {} is low",
                        device.getSourceId(), device.getUserId());
                break;
            case CRITICAL:
                logger.warn("Battery level of sensor {} of user {} is critically low",
                        device.getSourceId(), device.getUserId());
                break;
            case EMPTY:
                logger.error("Battery of sensor {} of user {} is empty",
                        device.getSourceId(), device.getUserId());
                break;
        }
    }
}
