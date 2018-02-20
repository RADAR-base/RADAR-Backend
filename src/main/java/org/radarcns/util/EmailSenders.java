package org.radarcns.util;

import org.radarcns.config.MonitorConfig;
import org.radarcns.config.NotifyConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


/**
 * Class to store {@link EmailSender} associated with each project.
 */
public class EmailSenders {

    private final Map<String,EmailSender> emailSenderMap;

    public EmailSenders(Map<String, EmailSender> map) {
        this.emailSenderMap = map;
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

    public static EmailSenders parseConfig(MonitorConfig config) throws IOException{
        Map<String, EmailSender> map = new HashMap<>();
        for(NotifyConfig notifyConfig : config.getNotifyConfig()) {
            map.put(notifyConfig.getProjectId(),
                    new EmailSender(config.getEmailHost(), config.getEmailPort(),
                            config.getEmailUser(), notifyConfig.getEmailAddress()));
        }

        return new EmailSenders(map);
    }

    public EmailSender getEmailSenderForProject(String projectId) {
        return emailSenderMap.get(projectId);
    }

}
