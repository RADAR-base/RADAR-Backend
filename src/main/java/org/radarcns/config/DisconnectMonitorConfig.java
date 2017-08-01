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

/**
 * POJO representing a disconnection status monitor configuration
 */
public class DisconnectMonitorConfig extends MonitorConfig {
    private Long timeout; // 5 minutes

    @JsonProperty("repeatitive_alert_delay")
    private Long repetitiveAlertDelay;

    public Long getTimeout() {
        return timeout;
    }

    public void setTimeout(Long timeout) {
        this.timeout = timeout;
    }

    public Long getRepetitiveAlertDelay() {
        return repetitiveAlertDelay;
    }

    public void setRepetitiveAlertDelay(Long repetitiveAlertDelay) {
        this.repetitiveAlertDelay = repetitiveAlertDelay;
    }
}

