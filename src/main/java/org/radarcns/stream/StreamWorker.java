package org.radarcns.stream;

import java.util.stream.Stream;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.config.SingleStreamConfig;

/** Stream worker. */
public interface StreamWorker {
    /** Configure the stream. */
    void configure(StreamMaster streamMaster, RadarPropertyHandler properties,
            SingleStreamConfig singleConfig);

    /**
     * Start work. This needs to be called after
     * {@link #configure(StreamMaster, RadarPropertyHandler, SingleStreamConfig)}.
     */
    void start();

    /** Get the stream definition that this worker is processing. */
    Stream<StreamDefinition> getStreamDefinitions();

    /** Shut down work. */
    void shutdown();
}
