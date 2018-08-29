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

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * POJO representing the yml file
 */
public class ConfigRadar {
    private Date released;
    private String version;
    private List<ServerConfig> zookeeper;
    private List<ServerConfig> broker;
    @JsonProperty("schema_registry")
    private List<ServerConfig> schemaRegistry;
    @JsonProperty("rest_proxy")
    private ServerConfig restProxy;
    @JsonProperty("battery_monitor")
    private BatteryMonitorConfig batteryMonitor;
    @JsonProperty("disconnect_monitor")
    private DisconnectMonitorConfig disconnectMonitor;
    @JsonProperty("statistics_monitors")
    private List<SourceStatisticsMonitorConfig> statisticsMonitors;
    @JsonProperty("stream")
    private StreamConfig stream;
    @JsonProperty("persistence_path")
    private String persistencePath;
    private Map<String, Object> extras;

    @JsonProperty("build_version")
    private String buildVersion;

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

    @Override
    public String toString() {
        return new YamlConfigLoader().prettyString(this);
    }

    public String getBuildVersion() {
        return buildVersion;
    }

    public void setBuildVersion(String buildVersion) {
        this.buildVersion = buildVersion;
    }

    public List<SourceStatisticsMonitorConfig> getStatisticsMonitors() {
        return statisticsMonitors;
    }

    public void setStatisticsMonitors(List<SourceStatisticsMonitorConfig> statisticsMonitors) {
        this.statisticsMonitors = statisticsMonitors;
    }

    public StreamConfig getStream() {
        return stream;
    }
}
