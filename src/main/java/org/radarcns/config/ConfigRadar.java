package org.radarcns.config;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

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
    @JsonProperty("stream_priority")
    private Map<String,Integer> streamPriority;
    @JsonProperty("battery_monitor")
    private BatteryMonitorConfig batteryMonitor;
    @JsonProperty("disconnect_monitor")
    private DisconnectMonitorConfig disconnectMonitor;

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

    public Map<String, Integer> getStreamPriority() {
        return streamPriority;
    }

    public void setStreamPriority(Map<String, Integer> streamPriority) {
        this.streamPriority = new HashMap<>();
        for (Map.Entry<String, Integer> entry : streamPriority.entrySet()) {
            this.streamPriority.put(entry.getKey().toLowerCase(Locale.US), entry.getValue());
        }
    }

    public List<ServerConfig> getSchemaRegistry() {
        return schemaRegistry;
    }

    public void setSchemaRegistry(List<ServerConfig> schemaRegistry) {
        this.schemaRegistry = schemaRegistry;
    }

    public Integer threadsByPriority(RadarPropertyHandler.Priority level){
        return streamPriority.get(level.getParam());
    }

    public String getZookeeperPaths() {
        return ServerConfig.getPaths(zookeeper);
    }

    public String getBrokerPaths() {
        return ServerConfig.getPaths(broker);
    }

    public String getSchemaRegistryPaths() {
        return ServerConfig.getPaths(schemaRegistry);
    }

    public String infoThread(){
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

    @Override
    public String toString() {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        // pretty print
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        // make ConfigRadar the root element
        mapper.enable(SerializationFeature.WRAP_ROOT_VALUE);
        // only serialize fields, not getters, etc.
        mapper.setVisibility(mapper.getSerializationConfig().getDefaultVisibilityChecker()
                .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
                .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withIsGetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withSetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withCreatorVisibility(JsonAutoDetect.Visibility.NONE));

        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException ex) {
            throw new UnsupportedOperationException("Cannot serialize config", ex);
        }
    }
}
