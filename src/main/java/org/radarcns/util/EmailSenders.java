package org.radarcns.util;

import org.radarcns.config.MonitorConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class EmailSenders {

    private Map<String,EmailSender> emailSenderMap;

    public EmailSenders(MonitorConfig config) {
        emailSenderMap = new HashMap<>();
        addSender(config);
    }

    private void addSender(MonitorConfig config) {

        config.getNotifyConfig().stream().forEach(s -> {
            try {
                emailSenderMap.put(s.getProjectId(),
                        new EmailSender(config.getEmailHost(), config.getEmailPort(),
                                config.getEmailUser(), s.getEmailAddress()));
            } catch (IOException exc) {
                exc.printStackTrace();
            }
        });
    }

    public EmailSender getEmailSender(String projectId) {
        return emailSenderMap.get(projectId);

    }

    private void putEmailSender(String projectId, EmailSender sender) {
        emailSenderMap.put(projectId, sender);
    }

    public Map<String, EmailSender> getEmailSenderMap() {
        return emailSenderMap;
    }

    public void setEmailSenderMap(Map<String, EmailSender> emailSenderMap) {
        this.emailSenderMap = emailSenderMap;
    }
}
