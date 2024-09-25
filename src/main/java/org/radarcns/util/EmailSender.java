/*
 * Copyright 2017 King's College London and The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarcns.util;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import javax.annotation.Nonnull;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import org.radarcns.config.EmailServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sends emails.
 */
public class EmailSender {
    private static final Logger logger = LoggerFactory.getLogger(EmailSender.class);

    private final InternetAddress from;
    private final InternetAddress[] recipients;
    private final Session session;

    /**
     * Email sender to simple SMTP host. The host must not use authentication
     * @param host smtp host
     * @param port port that the smtp service is configured on
     * @param from MIME From header
     * @param to list of recipients in the MIME To header
     * @throws IOException if a connection cannot be established with the email provider.
     */
    public EmailSender(EmailServerConfig config, @Nonnull String from, List<String> to)
            throws IOException, AddressException {
        this(createSession(config), from, to);
    }

    public EmailSender(Session session, @Nonnull String from, List<String> to)
            throws AddressException {
        this.from = new InternetAddress(from);
        if (to == null || to.isEmpty()) {
            throw new AddressException("Cannot create email sender without recipients.");
        }
        this.recipients = new InternetAddress[to.size()];
        for (int i = 0; i < to.size(); i++) {
            String addr = to.get(i);
            if (addr == null) {
                throw new AddressException("Recipient email address is null");
            }
            recipients[i] = new InternetAddress(addr);
        }
        this.session = session;
    }

    public static Session createSession(EmailServerConfig config) throws IOException {
        String host = config.getHost();
        int port = config.getPort();

        if (host == null || port <= 0) {
            logger.error("Cannot configure email sender without hosts ({}), port ({})",
                    host, port);
            return null;
        }


        Properties properties = new Properties();
        // Get system properties
        properties.putAll(System.getProperties());

        // Setup mail server
        properties.setProperty("mail.smtp.host", host);
        properties.setProperty("mail.smtp.port", String.valueOf(port));

        Session session = Session.getInstance(properties);
        try {
            Transport transport = session.getTransport("smtp");
            transport.connect();
            if (!transport.isConnected()) {
                throw new IOException("Cannot connect to SMTP server "
                        + config.getHost() + ":" + config.getPort());
            }
        } catch (MessagingException ex) {
            throw new IOException("Cannot instantiate SMTP server", ex);
        }
        return session;
    }

    /**
     * Send an email with given subject and text. The pre-configured From and To headers are used.
     * @param subject email subject
     * @param text plain text content of the email
     * @throws MessagingException if the message could not be sent
     */
    public void sendEmail(String subject, String text) throws MessagingException {
        // Create a default MimeMessage object.
        MimeMessage message = new MimeMessage(session);

        // Set From: header field of the header.
        message.setFrom(from);

        message.addRecipients(Message.RecipientType.TO, recipients);

        // Set Subject: header field
        message.setSubject(subject);

        // Now set the actual message
        message.setText(text);

        // Send message
        Transport.send(message);
    }
}
