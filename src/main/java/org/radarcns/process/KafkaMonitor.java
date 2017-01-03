package org.radarcns.process;

import org.radarcns.config.SubCommand;

public interface KafkaMonitor extends SubCommand {
    boolean isShutdown();
    long getPollTimeout();
    void setPollTimeout(long pollTimeout);
}
