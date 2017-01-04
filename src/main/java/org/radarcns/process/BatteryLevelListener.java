package org.radarcns.process;

import org.radarcns.key.MeasurementKey;

public interface BatteryLevelListener {
    enum Status {
        NORMAL(1.0f), LOW(0.2f), CRITICAL(0.05f), EMPTY(0f);

        private final float level;

        Status(float level) {
            this.level = level;
        }

        public float getLevel() {
            return this.level;
        }
    }
    void batteryLevelStatusUpdated(MeasurementKey device, Status status);
}
