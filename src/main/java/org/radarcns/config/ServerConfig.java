package org.radarcns.config;

import java.util.List;
import java.util.stream.Collectors;

/**
 * POJO representing a ServerConfig configuration
 */
public class ServerConfig {
    private String host;
    private Integer port;
    private String protocol;

    public String getHost() {
        return host;
    }

    public Integer getPort() {
        return port;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getPath() {
        if (protocol != null) {
            return protocol +"://"+ host + ":" + port;
        } else {
            return host + ":" + port;
        }
    }

    @Override
    public String toString() {
        return getPath();
    }

    public static String getPaths(List<ServerConfig> configList) {
        return configList.stream().map(ServerConfig::getPath).collect(Collectors.joining(","));
    }
}
