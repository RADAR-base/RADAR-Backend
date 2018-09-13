package org.radarcns.stream;

import java.util.stream.Stream;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.config.SingleStreamConfig;

public interface StreamWorker {
    void start();
    void configure(StreamMaster streamMaster, RadarPropertyHandler properties,
            SingleStreamConfig singleConfig);
    Stream<StreamDefinition> getStreamDefinitions();
    void shutdown();
}
