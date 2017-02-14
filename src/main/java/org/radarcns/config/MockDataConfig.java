package org.radarcns.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.File;

public class MockDataConfig {
    private String topic;
    @JsonProperty("key_schema")
    private String keySchema;
    @JsonProperty("value_schema")
    private String valueSchema;
    @JsonProperty("file")
    private String dataFile;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getKeySchema() {
        return keySchema;
    }

    public void setKeySchema(String keySchema) {
        this.keySchema = keySchema;
    }

    public String getValueSchema() {
        return valueSchema;
    }

    public void setValueSchema(String valueSchema) {
        this.valueSchema = valueSchema;
    }

    public File getDataFile(File configFile) {
        File directDataFile = new File(dataFile);
        if (directDataFile.isAbsolute()) {
            return directDataFile;
        } else {
            return new File(configFile.getParentFile(), dataFile);
        }
    }

    public String getDataFile() {
        return dataFile;
    }

    public void setDataFile(String dataFile) {
        this.dataFile = dataFile;
    }
}
