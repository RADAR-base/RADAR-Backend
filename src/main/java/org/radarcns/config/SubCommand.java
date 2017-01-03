package org.radarcns.config;

import java.io.IOException;

/**
 *
 */
public interface SubCommand {
    void start() throws IOException;
    void shutdown() throws IOException, InterruptedException;
}
