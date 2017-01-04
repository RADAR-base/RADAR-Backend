package org.radarcns.config;

import java.io.IOException;

/**
 * Subcommand of RadarBackend to run.
 */
public interface SubCommand {
    /** Start the subcommand */
    void start() throws IOException;
    /** Stop the subcommand, possibly waiting for it to complete. */
    void shutdown() throws IOException, InterruptedException;
}
