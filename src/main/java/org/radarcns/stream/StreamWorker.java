package org.radarcns.stream;

import java.util.stream.Stream;

public interface StreamWorker {
    void start();
    Stream<StreamDefinition> getStreamDefinitions();
    void shutdown();
}
