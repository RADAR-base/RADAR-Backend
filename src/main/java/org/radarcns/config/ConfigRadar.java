package org.radarcns.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

    public ConfigRadar() {
    }

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
        this.streamPriority = streamPriority;
    }

    public List<ServerConfig> getSchema_registry() {
        return schemaRegistry;
    }

    public void setSchema_registry(List<ServerConfig> schemaRegistry) {
        this.schemaRegistry = schemaRegistry;
    }

    public Integer threadsByPriority(RadarPropertyHandler.Priority level){
        return streamPriority.get(level.getParam());
    }

    public String getZookeeperPath(){
        return zookeeper.get(0).getPath();
    }

    public String getBrokerPath(){
        return broker.get(0).getPath();
    }

    public String getSchemaRegistryPath(){
        return schemaRegistry.get(0).getPath();
    }

    public String infoThread(){
        String tab = "  ";
        return "{" + "\n" + streamPriority.keySet().stream().map(item -> tab + tab + item.toLowerCase() + "=" + stream_priority.get(item)).collect(Collectors.joining(" \n")) + "\n" + tab + "}";
    }

    @Override
    public String toString() {
        return "Settings{" + "\n" +
                "  " + "released=" + released + "\n" +
                "  " + "version='" + version + '\'' + "\n" +
                "  " + "logPath='" + logPath + '\'' + "\n" +
                "  " + "mode='" + mode + '\'' + "\n" +
                "  " + "zookeeper=" + zookeeper + "\n" +
                "  " + "broker=" + broker + "\n" +
                "  " + "schemaRegistry=" + schemaRegistry + "\n" +
                "  " + "autoCommitIntervalMs=" + autoCommitIntervalMs + "\n" +
                "  " + "sessionTimeoutMs=" + sessionTimeoutMs + "\n" +
                "  " + "streamPriority=" + streamPriority + "\n" +
                '}';
    }

    public String info() {

        String tab = "  ";

        return "Settings{" + "\n" +
                tab + "released=" + released + "\n" +
                tab + "version='" + version + '\'' + "\n" +
                tab + "logPath='" + logPath + '\'' + "\n" +
                tab + "mode='" + mode + '\'' + "\n" +
                tab + "zookeeper={" + "\n" + zookeeper.stream().map(item -> tab + tab + item.info()).collect(Collectors.joining(" \n")) + "\n" + tab + "}" + "\n" +
                tab + "broker={" + "\n" + broker.stream().map(item -> tab + tab + item.info()).collect(Collectors.joining(" \n")) + "\n" + tab + "}" + "\n" +
                tab + "schemaRegistry={" + "\n" + schemaRegistry.stream().map(item -> tab + tab + item.info()).collect(Collectors.joining(" \n")) + "\n" + tab + "}" + "\n" +
                tab + "autoCommitIntervalMs=" + autoCommitIntervalMs + "\n" +
                tab + "sessionTimeoutMs=" + sessionTimeoutMs + "\n" +
                tab + "streamPriority=" + infoThread() + "\n" +
                '}';
    }
}
