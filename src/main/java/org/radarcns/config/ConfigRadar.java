/*
 * Copyright 2017 King's College London and The Hyve
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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Date;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.radarcns.config.RadarPropertyHandler.Priority;
import org.radarcns.stream.aggregator.MasterAggregator;

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
    @JsonProperty("rest_proxy")
    private ServerConfig restProxy;
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
    @JsonProperty("stream_worker")
    private String streamWorker;
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

    public ServerConfig getRestProxy() {
        return restProxy;
    }

    public void setRestProxy(ServerConfig restProxy) {
        this.restProxy = restProxy;
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

    public String getRestProxyPath() {
        if (restProxy == null) {
            throw new IllegalStateException("'rest_proxy' is not configured");
        }

        return restProxy.getPath();
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

    public String getStreamWorker() {
        return streamWorker;
    }

    public void setStreamWorker(String streamWorker) {
        streamWorker = streamWorker;
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

    @Override
    public String toString() {
        return new YamlConfigLoader().prettyString(this);
    }
}
