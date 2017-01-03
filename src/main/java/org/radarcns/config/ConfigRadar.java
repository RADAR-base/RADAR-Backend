package org.radarcns.config;

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
    private String log_path;
    private String mode;
    private List<ServerConfig> zookeeper;
    private List<ServerConfig> broker;
    private List<ServerConfig> schema_registry;
    private Integer auto_commit_interval_ms;
    private Integer session_timeout_ms;
    private Map<String,Integer> stream_priority;

    public ConfigRadar() {}

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

    public String getLog_path() {
        return log_path;
    }

    public void setLog_path(String log_path) {
        this.log_path = log_path;
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

    public Integer getAuto_commit_interval_ms() {
        return auto_commit_interval_ms;
    }

    public void setAuto_commit_interval_ms(Integer auto_commit_interval_ms) {
        this.auto_commit_interval_ms = auto_commit_interval_ms;
    }

    public Integer getSession_timeout_ms() {
        return session_timeout_ms;
    }

    public void setSession_timeout_ms(Integer session_timeout_ms) {
        this.session_timeout_ms = session_timeout_ms;
    }

    public Map<String, Integer> getStream_priority() {
        return stream_priority;
    }

    public void setStream_priority(Map<String, Integer> stream_priority) {
        this.stream_priority = stream_priority;
    }

    public List<ServerConfig> getSchema_registry() {
        return schema_registry;
    }

    public void setSchema_registry(List<ServerConfig> schema_registry) {
        this.schema_registry = schema_registry;
    }

    public Integer threadsByPriority(RadarPropertyHandler.Priority level){
        return stream_priority.get(level.getParam());
    }

    public String getZookeeperPath(){
        return zookeeper.get(0).getPath();
    }

    public String getBrokerPath(){
        return broker.get(0).getPath();
    }

    public String getSchemaRegistryPath(){
        return schema_registry.get(0).getPath();
    }

    public String infoThread(){
        String tab = "  ";
        return "{" + "\n" + stream_priority.keySet().stream().map(item -> tab + tab + item.toLowerCase() + "=" + stream_priority.get(item)).collect(Collectors.joining(" \n")) + "\n" + tab + "}";
    }

    @Override
    public String toString() {
        return "Settings{" + "\n" +
                "  " + "released=" + released + "\n" +
                "  " + "version='" + version + '\'' + "\n" +
                "  " + "log_path='" + log_path + '\'' + "\n" +
                "  " + "mode='" + mode + '\'' + "\n" +
                "  " + "zookeeper=" + zookeeper + "\n" +
                "  " + "broker=" + broker + "\n" +
                "  " + "schema_registry=" + schema_registry + "\n" +
                "  " + "auto_commit_interval_ms=" + auto_commit_interval_ms + "\n" +
                "  " + "session_timeout_ms=" + session_timeout_ms + "\n" +
                "  " + "streams_priority=" + stream_priority + "\n" +
                '}';
    }

    public String info() {

        String tab = "  ";

        return "Settings{" + "\n" +
                tab + "released=" + released + "\n" +
                tab + "version='" + version + '\'' + "\n" +
                tab + "log_path='" + log_path + '\'' + "\n" +
                tab + "mode='" + mode + '\'' + "\n" +
                tab + "zookeeper={" + "\n" + zookeeper.stream().map(item -> tab + tab + item.info()).collect(Collectors.joining(" \n")) + "\n" + tab + "}" + "\n" +
                tab + "broker={" + "\n" + broker.stream().map(item -> tab + tab + item.info()).collect(Collectors.joining(" \n")) + "\n" + tab + "}" + "\n" +
                tab + "schema_registry={" + "\n" + schema_registry.stream().map(item -> tab + tab + item.info()).collect(Collectors.joining(" \n")) + "\n" + tab + "}" + "\n" +
                tab + "auto_commit_interval_ms=" + auto_commit_interval_ms + "\n" +
                tab + "session_timeout_ms=" + session_timeout_ms + "\n" +
                tab + "streams_priority=" + infoThread() + "\n" +
                '}';
    }
}
