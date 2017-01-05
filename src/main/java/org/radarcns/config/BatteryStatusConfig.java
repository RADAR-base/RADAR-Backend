package org.radarcns.config;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * POJO representing a battery status monitor configuration
 */
public class BatteryStatusConfig {
    @JsonProperty("email_address")
    private String emailAddress;
    private String level;

    public String getEmailAddress() {
        return emailAddress;
    }

    public void setEmailAddress(String emailAddress) {
        this.emailAddress = emailAddress;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }
}
