package org.radarcns.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class NotifyConfig {
    @JsonProperty("project_id")
    private String projectId;

    @JsonProperty("email_address")
    private List<String> emailAddress;


    public String getProjectId() {
        return projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public List<String> getEmailAddress() {
        return emailAddress;
    }

    public void setEmailAddress(List<String> email_address) {
        this.emailAddress = email_address;
    }
}
