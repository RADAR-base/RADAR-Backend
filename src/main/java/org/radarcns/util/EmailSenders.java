package org.radarcns.util;

import org.radarcns.config.MonitorConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


/**
 * Class to store {@link EmailSender} associated with each project.
 */
public class EmailSenders {

    private Map<String,EmailSender> emailSenderMap;
    private IOException exception;

    /**
     * Default Constructor
     */
    public EmailSenders() {
        emailSenderMap = new HashMap<>();
    }

    /**
     * Initialize the {@link EmailSender} map using the configuration.
     *
     * @param  config  Configuration of the Monitor containing project
     *                 and email address mapping
     * @throws IOException
     */
    public EmailSenders(MonitorConfig config) throws IOException{
        emailSenderMap = new HashMap<>();
        addSender(config);
    }

    /**
     *
     * Parses the {@link MonitorConfig} to map the corresponding
     * {@link EmailSender} to each project. A project can have a list of
     * associated email addresses.
     *
     * @param  config  Configuration of the Monitor containing project
     *                 and email address mapping
     * @throws IOException
     */

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

        if(exception != null) {
            throw exception;
        }
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
