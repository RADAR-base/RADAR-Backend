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
import org.radarcns.config.EmailServerConfig;

import java.util.List;

/**
 * POJO representing a monitor configuration
 */
public class MonitorConfig {
    @JsonProperty("notify")
    private List<EmailNotifyConfig> emailNotifyConfig;

    @JsonProperty("email_server")
    private EmailServerConfig emailServerConfig;

    @JsonProperty("log_interval")
    private int logInterval = 1000;

    private List<String> topics;

    @JsonProperty("message")
    private String message = null;

    public List<String> getTopics() {
        return topics;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
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

    public EmailServerConfig getEmailServerConfig() {
        return emailServerConfig;
    }

    public void setEmailServerConfig(EmailServerConfig emailServerConfig) {
        this.emailServerConfig = emailServerConfig;
    }

    public List<EmailNotifyConfig> getEmailNotifyConfig() {
        return emailNotifyConfig;
    }

    public void setEmailNotifyConfig(List<EmailNotifyConfig> emailNotifyConfig) {
        this.emailNotifyConfig = emailNotifyConfig;
    }
}
