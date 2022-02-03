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
import org.radarcns.config.monitor.MonitorConfig;

/**
 * POJO representing a disconnection status monitor configuration.
 */
public class DisconnectMonitorConfig extends MonitorConfig {
    private long timeout = 1800L; // 30 minutes

    @JsonProperty("alert_repeat_interval")
    private long alertRepeatInterval = 86400; // 1 day

    @JsonProperty("alert_repetitions")
    private int alertRepetitions = 0;

    public Long getTimeout() {
        return timeout;
    }

    public void setTimeout(Long timeout) {
        this.timeout = timeout;
    }

    public long getAlertRepeatInterval() {
        return alertRepeatInterval;
    }

    public void setAlertRepeatInterval(long alertRepeatInterval) {
        this.alertRepeatInterval = alertRepeatInterval;
    }

    public int getAlertRepetitions() {
        return alertRepetitions;
    }

    public void setAlertRepetitions(int alertRepetitions) {
        this.alertRepetitions = alertRepetitions;
    }
}
