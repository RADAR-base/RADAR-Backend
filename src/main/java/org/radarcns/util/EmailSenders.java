package org.radarcns.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.mail.internet.AddressException;
import org.radarcns.config.EmailServerConfig;
import org.radarcns.config.monitor.MonitorConfig;
import org.radarcns.config.monitor.EmailNotifyConfig;
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
        return parseConfig(config.getEmailServerConfig(), config.getEmailNotifyConfig());
    }

    /**
     * Parses the {@link MonitorConfig} to map the corresponding
     * {@link EmailSender} to each project. A project can have a list of
     * associated email addresses.
     *
     * @param  serverConfig  Email server configuration
     * @param  notifyConfigs  Configuration of the Monitor containing project
     *                 and email address mapping
     * @throws IOException if a connection cannot be established with the email provider.
     */
    public static EmailSenders parseConfig(EmailServerConfig serverConfig,
            List<EmailNotifyConfig> notifyConfigs) throws IOException {
        String host = serverConfig.getHost();
        int port = serverConfig.getPort();
        String user = serverConfig.getUser();

        if (host == null || user == null || port <= 0) {
            logger.error("Cannot configure email sender without hosts ({}), port ({}) or user ({})",
                    host, user, port);
            return new EmailSenders(Map.of());
        }

        Map<String, EmailSender> map = new HashMap<>();

        for (EmailNotifyConfig emailNotifyConfig : notifyConfigs) {
            try {
                map.put(emailNotifyConfig.getProjectId(),
                        new EmailSender(host, port, user, emailNotifyConfig.getEmailAddress()));
            } catch (AddressException e) {
                logger.error("Failed to add email sender for addresses {} and {}",
                        user, emailNotifyConfig.getEmailAddress(), e);
            }
        }

        return new EmailSenders(map);
    }

    public EmailSender getEmailSenderForProject(String projectId) {
        return emailSenderMap.get(projectId);
    }

}
