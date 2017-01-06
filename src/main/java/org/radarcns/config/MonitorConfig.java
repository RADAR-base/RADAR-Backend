package org.radarcns.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * POJO representing a monitor configuration
 */
public class MonitorConfig {
    @JsonProperty("email_address")
    private String emailAddress;
    private List<String> topics;

    public String getEmailAddress() {
        return emailAddress;
    }

    public void setEmailAddress(String emailAddress) {
        this.emailAddress = emailAddress;
    }

    public List<String> getTopics() {
        return topics;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }
}
