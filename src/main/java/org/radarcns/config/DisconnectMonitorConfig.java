package org.radarcns.config;

/**
 * POJO representing a disconnection status monitor configuration
 */
public class DisconnectMonitorConfig extends MonitorConfig {
    private Long timeout; // 5 minutes

    public Long getTimeout() {
        return timeout;
    }

    public void setTimeout(Long timeout) {
        this.timeout = timeout;
    }
}
