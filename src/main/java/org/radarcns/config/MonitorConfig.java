package org.radarcns.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * POJO representing a monitor configuration
 */
public class MonitorConfig {
    @JsonProperty("email_address")
    private String emailAddress;

    @JsonProperty("email_host")
    private String emailHost;

    @JsonProperty("email_port")
    private int emailPort;

    @JsonProperty("email_user")
    private String emailUser;

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
}
