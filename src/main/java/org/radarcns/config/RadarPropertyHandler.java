package org.radarcns.config;

import java.io.IOException;

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
}
