package org.radarcns.config;

/**
 * POJO representing a battery status monitor configuration
 */
public class BatteryMonitorConfig extends MonitorConfig {
    private String level;

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }
}
