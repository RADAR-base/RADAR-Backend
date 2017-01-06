package org.radarcns.util;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import javax.mail.Address;
import javax.mail.Message.RecipientType;
import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.subethamail.wiser.Wiser;
import org.subethamail.wiser.WiserMessage;

public class EmailSenderTest {
    private Wiser emailServer;

    @Before
    public void setUp() {
        emailServer = new Wiser(2525);
        emailServer.setHostname("localhost");
        emailServer.start();
    }

    @After
    public void tearDown() {
        emailServer.stop();
    }

    @Test
    public void testEmail() throws MessagingException, IOException {
        EmailSender sender = new EmailSender("localhost", 2525, "no-reply@radar-cns.org",
                Collections.singletonList("test@radar-cns.org"));

        assertEquals(Collections.emptyList(), emailServer.getMessages());

        sender.sendEmail("hi", "it's me");

        List<WiserMessage> messages = emailServer.getMessages();
        assertEquals(1, messages.size());
        WiserMessage message = messages.get(0);
        MimeMessage mime = message.getMimeMessage();

        assertEquals(1, mime.getFrom().length);
        assertEquals("no-reply@radar-cns.org", mime.getFrom()[0].toString());

        Address[] to = mime.getRecipients(RecipientType.TO);
        assertEquals(1, to.length);
        assertEquals("test@radar-cns.org", to[0].toString());

        assertEquals("hi", mime.getSubject());
        assertEquals("it's me", mime.getContent().toString().trim());
    }
}