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

package org.radarcns.config.monitor;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * POJO representing a monitor configuration
 */
public class MonitorConfig {
    @JsonProperty("notify")
    private List<EmailNotifyConfig> emailNotifyConfig;

    @JsonProperty("email_host")
    private String emailHost;

    @JsonProperty("email_port")
    private int emailPort;

    @JsonProperty("email_user")
    private String emailUser;

    @JsonProperty("log_interval")
    private int logInterval = 1000;

    private List<String> topics;

    @JsonProperty("message")
    private String message = null;

    public List<EmailNotifyConfig> getNotifyConfig() {
        return emailNotifyConfig;
    }

    public void setNotifyConfig(List<EmailNotifyConfig> emailNotifyConfig) {
        this.emailNotifyConfig = emailNotifyConfig;
    }

    public List<String> getTopics() {
        return topics;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

    public String getEmailHost() {
        return emailHost;
    }

    public void setEmailHost(String emailHost) {
        this.emailHost = emailHost;
    }

    public int getEmailPort() {
        return emailPort;
    }

    public void setEmailPort(int emailPort) {
        this.emailPort = emailPort;
    }

    public String getEmailUser() {
        return emailUser;
    }

    public void setEmailUser(String emailUser) {
        this.emailUser = emailUser;
    }

    public int getLogInterval() {
        return logInterval;
    }

    public void setLogInterval(int logInterval) {
        this.logInterval = logInterval;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
