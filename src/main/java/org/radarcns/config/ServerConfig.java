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

import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

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
            return protocol + "://" + host + ":" + port;
        } else {
            return host + ":" + port;
        }
    }

    @Override
    public String toString() {
        return getPath();
    }

    public static String getPaths(@Nonnull List<ServerConfig> configList) {
        return configList.stream().map(ServerConfig::getPath).collect(Collectors.joining(","));
    }
}
