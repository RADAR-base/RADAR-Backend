package org.radarcns.util;

import java.util.List;
import java.util.Properties;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

public class EmailSender {
    private final String from;
    private final List<String> to;
    private final Properties properties;

    public EmailSender(String host, int port, String from, List<String> to) {
        this.from = from;
        this.to = to;

        // Get system properties
        properties = System.getProperties();

        // Setup mail server
        properties.setProperty("mail.smtp.host", host);
        properties.setProperty("mail.smtp.port", String.valueOf(port));
    }

    public void sendEmail(String subject, String text) throws MessagingException {

        // Get the default Session object.
        Session session = Session.getDefaultInstance(properties);

        // Create a default MimeMessage object.
        MimeMessage message = new MimeMessage(session);

        // Set From: header field of the header.
        message.setFrom(new InternetAddress(from));

        for (String recipient : to) {
            // Set To: header field of the header.
            message.addRecipient(Message.RecipientType.TO, new InternetAddress(recipient));
        }

        // Set Subject: header field
        message.setSubject(subject);

        // Now set the actual message
        message.setText(text);

        // Send message
        Transport.send(message);
    }
}
