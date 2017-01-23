/*
 * Copyright 2017 Kings College London and The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarcns.config;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.radarcns.config.RadarPropertyHandler.Priority;

/**
 * POJO representing the yml file
 */
public class ConfigRadar {
    private Date released;
    private String version;
    @JsonProperty("log_path")
    private String logPath;
    private String mode;
    private List<ServerConfig> zookeeper;
    private List<ServerConfig> broker;
    @JsonProperty("schema_registry")
    private List<ServerConfig> schemaRegistry;
    @JsonProperty("auto_commit_interval_ms")
    private Integer autoCommitIntervalMs;
    @JsonProperty("session_timeout_ms")
    private Integer sessionTimeoutMs;
    @JsonIgnore
    private Map<Priority, Integer> streamPriority;
    @JsonProperty("battery_monitor")
    private BatteryMonitorConfig batteryMonitor;
    @JsonProperty("disconnect_monitor")
    private DisconnectMonitorConfig disconnectMonitor;
    @JsonProperty("persistence_path")
    private String persistencePath;
    private Map<String, Object> extras;

    public Date getReleased() {
        return released;
    }

    public void setReleased(Date released) {
        this.released = released;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getLogPath() {
        return logPath;
    }

    public void setLogPath(String logPath) {
        this.logPath = logPath;
    }

    public boolean isStandalone() {
        return mode.equals("standalone");
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getMode() {
        return mode;
    }

    public List<ServerConfig> getZookeeper() {
        return zookeeper;
    }

    public void setZookeeper(List<ServerConfig> zookeeper) {
        this.zookeeper = zookeeper;
    }

    public List<ServerConfig> getBroker() {
        return broker;
    }

    public void setBroker(List<ServerConfig> broker) {
        this.broker = broker;
    }

    public Integer getAutoCommitIntervalMs() {
        return autoCommitIntervalMs;
    }

    public void setAutoCommitIntervalMs(Integer autoCommitIntervalMs) {
        this.autoCommitIntervalMs = autoCommitIntervalMs;
    }

    public Integer getSessionTimeoutMs() {
        return sessionTimeoutMs;
    }

    public void setSessionTimeoutMs(Integer sessionTimeoutMs) {
        this.sessionTimeoutMs = sessionTimeoutMs;
    }

    @JsonProperty("stream_priority")
    public Map<String, Integer> getStreamPriority() {
        if (streamPriority == null) {
            return null;
        }
        Map<String, Integer> stringStreamPriority = new HashMap<>();
        for (Map.Entry<Priority, Integer> entry : streamPriority.entrySet()) {
            stringStreamPriority.put(entry.getKey().getParam(), entry.getValue());
        }
        return stringStreamPriority;
    }

    @JsonProperty("stream_priority")
    public void setStreamPriority(Map<String, Integer> streamPriority) {
        if (streamPriority == null) {
            this.streamPriority = null;
            return;
        }

        this.streamPriority = new EnumMap<>(Priority.class);

        for (Map.Entry<String, Integer> entry : streamPriority.entrySet()) {
            if (entry.getValue() < 1) {
                throw new IllegalArgumentException("Stream priorities cannot be smaller than 1");
            }
            Priority priority = Priority.valueOf(entry.getKey().toUpperCase(Locale.US));
            this.streamPriority.put(priority, entry.getValue());
        }
    }

    public List<ServerConfig> getSchemaRegistry() {
        return schemaRegistry;
    }

    public void setSchemaRegistry(List<ServerConfig> schemaRegistry) {
        this.schemaRegistry = schemaRegistry;
    }

    public int threadsByPriority(Priority level, int defaultValue) {
        if (defaultValue <= 0) {
            throw new IllegalArgumentException("Default number of threads must be larger than 0");
        }
        if (streamPriority == null) {
            return defaultValue;
        }
        Integer mapValue = streamPriority.get(level);
        if (mapValue == null) {
            return defaultValue;
        }
        return mapValue;
    }

    public String getZookeeperPaths() {
        if (zookeeper == null) {
            throw new IllegalStateException("'zookeeper' is not configured");
        }
        return ServerConfig.getPaths(zookeeper);
    }

    public String getBrokerPaths() {
        if (broker == null) {
            throw new IllegalStateException("Kafka 'broker' is not configured");
        }
        return ServerConfig.getPaths(broker);
    }

    public String getSchemaRegistryPaths() {
        if (schemaRegistry == null) {
            throw new IllegalStateException("'schema_registry' is not configured");
        }

        return ServerConfig.getPaths(schemaRegistry);
    }

    public String infoThread() {
        return streamPriority.toString();
    }

    public BatteryMonitorConfig getBatteryMonitor() {
        return batteryMonitor;
    }

    public void setBatteryMonitor(BatteryMonitorConfig batteryMonitor) {
        this.batteryMonitor = batteryMonitor;
    }

    public DisconnectMonitorConfig getDisconnectMonitor() {
        return disconnectMonitor;
    }

    public void setDisconnectMonitor(DisconnectMonitorConfig disconnectMonitor) {
        this.disconnectMonitor = disconnectMonitor;
    }

    public String getPersistencePath() {
        return persistencePath;
    }

    public void setPersistencePath(String persistencePath) {
        this.persistencePath = persistencePath;
    }

    public Map<String, Object> getExtras() {
        return extras;
    }

    public void setExtras(Map<String, Object> extras) {
        this.extras = extras;
    }

    public static ConfigRadar load(File file) throws IOException {
        ObjectMapper mapper = createMapper();
        return mapper.readValue(file, ConfigRadar.class);
    }

    public void store(File file) throws IOException {
        ObjectMapper mapper = createMapper();
        mapper.writeValue(file, this);
    }

    private static ObjectMapper createMapper() {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
        // only serialize fields, not getters, etc.
        mapper.setVisibility(mapper.getSerializationConfig().getDefaultVisibilityChecker()
                .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
                .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withIsGetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withSetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withCreatorVisibility(JsonAutoDetect.Visibility.NONE));
        return mapper;
    }

    @Override
    public String toString() {
        ObjectMapper mapper = createMapper();
        // pretty print
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        // make ConfigRadar the root element
        mapper.enable(SerializationFeature.WRAP_ROOT_VALUE);

        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException ex) {
            throw new UnsupportedOperationException("Cannot serialize config", ex);
        }
    }
}
