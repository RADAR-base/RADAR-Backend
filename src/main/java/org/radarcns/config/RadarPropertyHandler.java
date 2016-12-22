package org.radarcns.config;

/**
 * Created by nivethika on 21-12-16.
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

    void load(String pathFile) throws Exception;

    KafkaProperty getKafkaProperties();
}
