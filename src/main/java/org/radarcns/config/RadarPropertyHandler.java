package org.radarcns.config;

import java.io.IOException;
import org.radarcns.util.PersistentStateStore;

/**
 * Interface that handles YAML configuration file loading
 */
public interface RadarPropertyHandler {

    enum Priority {
        LOW("low"), NORMAL("normal"), HIGH("high");

        private final String param;

        Priority(String param) {
            this.param = param;
        }

        public String getParam() {
            return param;
        }
    }

    ConfigRadar getRadarProperties();

    void load(String pathFile) throws IOException;

    KafkaProperty getKafkaProperties();

    /**
     * Create a {@link PersistentStateStore} if so configured in the radar properties. Notably, the
     * {@code persistent_directory} must be set.
     * @return PersistentStateStore or null if none is configured.
     * @throws IOException if the persistence store cannot be reached.
     */
    PersistentStateStore getPersistentStateStore() throws IOException;
}
