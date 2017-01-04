package org.radarcns.process;

import java.util.Properties;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import org.radarcns.key.MeasurementKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatteryLevelEmail implements BatteryLevelListener {
    private final String emailAddress;
    private final Status minLevel;
    private final static Logger logger = LoggerFactory.getLogger(BatteryLevelEmail.class);

    public BatteryLevelEmail(String emailAddress, Status minLevel) {
        this.emailAddress = emailAddress;
        this.minLevel = minLevel;
    }

    @Override
    public void batteryLevelStatusUpdated(MeasurementKey device, Status status) {
        if (status.getLevel() <= minLevel.getLevel()) {
            // Sender's email ID needs to be mentioned
            String from = "no-reply@radar-cns.org";

            // Assuming you are sending email from localhost
            String host = "localhost";

            // Get system properties
            Properties properties = System.getProperties();

            // Setup mail server
            properties.setProperty("mail.smtp.host", host);

            // Get the default Session object.
            Session session = Session.getDefaultInstance(properties);

            try {
                // Create a default MimeMessage object.
                MimeMessage message = new MimeMessage(session);

                // Set From: header field of the header.
                message.setFrom(new InternetAddress(from));

                // Set To: header field of the header.
                message.addRecipient(Message.RecipientType.TO, new InternetAddress(emailAddress));

                // Set Subject: header field
                message.setSubject("[RADAR-CNS] battery level low");

                // Now set the actual message
                message.setText(
                        "The battery level of " + device + " is now " + status + ". "
                        + "Please ensure that it gets recharged.");

                // Send message
                Transport.send(message);
                logger.info("Sent battery level status message successfully");
            } catch (MessagingException mex) {
                logger.error("Failed to send battery level status message.", mex);
            }
        }
    }
}
