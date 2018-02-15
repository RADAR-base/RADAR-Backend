package org.radarcns.util;

import org.radarcns.config.MonitorConfig;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;

public class EmailSenders {

    private Map<String,EmailSender> emailSenderMap;
    IOException exception;

    public EmailSenders() {
        emailSenderMap = new HashMap<>();
    }

    public EmailSenders(MonitorConfig config) throws IOException{
        emailSenderMap = new HashMap<>();
        addSender(config);
    }

    private void addSender(MonitorConfig config) throws IOException{
        exception = null;
        config.getNotifyConfig().stream().forEach(s -> {
            try {
                emailSenderMap.put(s.getProjectId(),
                        new EmailSender(config.getEmailHost(), config.getEmailPort(),
                                config.getEmailUser(), s.getEmailAddress()));
            } catch (IOException exc) {
                exception = exc;
            }
        });

        if(exception != null)
            throw exception;
    }



    public EmailSender getEmailSender(String projectId) {
        return emailSenderMap.get(projectId);
    }

    public void putEmailSender(String projectId, EmailSender sender) {
        emailSenderMap.put(projectId, sender);
    }

    public Map<String, EmailSender> getEmailSenderMap() {
        return emailSenderMap;
    }

    public void setEmailSenderMap(Map<String, EmailSender> emailSenderMap) {
        this.emailSenderMap = emailSenderMap;
    }



}
