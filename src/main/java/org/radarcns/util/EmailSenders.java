package org.radarcns.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.mail.internet.AddressException;
import org.radarcns.config.MonitorConfig;
import org.radarcns.config.NotifyConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class to store {@link EmailSender} associated with each project.
 */
public class EmailSenders {
    private static final Logger logger = LoggerFactory.getLogger(EmailSenders.class);

    private final Map<String,EmailSender> emailSenderMap;

    public EmailSenders(Map<String, EmailSender> map) {
        this.emailSenderMap = map;
    }

    /**
     * Parses the {@link MonitorConfig} to map the corresponding
     * {@link EmailSender} to each project. A project can have a list of
     * associated email addresses.
     *
     * @param  config  Configuration of the Monitor containing project
     *                 and email address mapping
     * @throws IOException if a connection cannot be established with the email provider.
     */

    public static EmailSenders parseConfig(MonitorConfig config) throws IOException {
        String host = config.getEmailHost();
        int port = config.getEmailPort();
        String user = config.getEmailUser();

        if (host == null || user == null || port <= 0) {
            logger.error("Cannot configure email sender without hosts ({}), port ({}) or user ({})",
                    host, user, port);
            return new EmailSenders(Map.of());
        }

        Map<String, EmailSender> map = new HashMap<>();

        for (NotifyConfig notifyConfig : config.getNotifyConfig()) {
            try {
                map.put(notifyConfig.getProjectId(),
                        new EmailSender(host, port, user, notifyConfig.getEmailAddress()));
            } catch (AddressException e) {
                logger.error("Failed to add email sender for addresses {} and {}",
                        config.getEmailUser(), notifyConfig.getEmailAddress(), e);
            }
        }

        return new EmailSenders(map);
    }

    public EmailSender getEmailSenderForProject(String projectId) {
        return emailSenderMap.get(projectId);
    }

}
