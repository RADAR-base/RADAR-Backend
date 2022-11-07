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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import javax.mail.Address;
import javax.mail.Message.RecipientType;
import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;
import org.junit.Rule;
import org.junit.Test;
import org.radarcns.config.EmailServerConfig;

public class EmailSenderTest {
    @Rule
    public EmailServerRule emailServer = new EmailServerRule(2525);

    @Test
    public void testEmail() throws MessagingException, IOException {
        EmailServerConfig emailServerConfig = new EmailServerConfig();
        emailServerConfig.setHost("localhost");
        emailServerConfig.setPort(emailServer.getPort());
        EmailSender sender = new EmailSender(emailServerConfig, "no-reply@radar-cns.org",
                List.of("test@radar-cns.org"));

        assertEquals(Collections.emptyList(), emailServer.getMessages());

        sender.sendEmail("hi", "it's me");

        List<MimeMessage> messages = emailServer.getMessages();
        assertEquals(1, messages.size());
        MimeMessage mime = messages.get(0);

        assertEquals(1, mime.getFrom().length);
        assertEquals("no-reply@radar-cns.org", mime.getFrom()[0].toString());

        Address[] to = mime.getRecipients(RecipientType.TO);
        assertEquals(1, to.length);
        assertEquals("test@radar-cns.org", to[0].toString());

        assertEquals("hi", mime.getSubject());
        assertEquals("it's me", mime.getContent().toString().trim());
    }

    @Test(expected = IOException.class)
    public void testEmailNonExisting() throws MessagingException, IOException {
        EmailServerConfig emailServerConfig = new EmailServerConfig();
        emailServerConfig.setHost("non-existing-host");
        emailServerConfig.setPort(emailServer.getPort());
        EmailSender sender = new EmailSender(emailServerConfig, "no-reply@radar-cns.org",
                List.of("test@radar-cns.org"));
    }
}
