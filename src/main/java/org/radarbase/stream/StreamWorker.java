package org.radarbase.stream;

import java.util.stream.Stream;
import org.radarbase.config.RadarPropertyHandler;
import org.radarbase.config.SingleStreamConfig;

public interface StreamWorker {
    void start();
    void configure(StreamMaster streamMaster, RadarPropertyHandler properties,
            SingleStreamConfig singleConfig);
    Stream<StreamDefinition> getStreamDefinitions();
    void shutdown();
}
