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

import java.util.ArrayList;
import java.util.List;
import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;
import org.junit.rules.ExternalResource;
import org.subethamail.wiser.Wiser;
import org.subethamail.wiser.WiserMessage;

public class EmailServerRule extends ExternalResource {
    private final int port;
    private Wiser emailServer;

    public EmailServerRule() {
        this(25251);
    }

    public EmailServerRule(int port) {
        this.port = port;
    }

    @Override
    protected void before() {
        emailServer = new Wiser(port);
        emailServer.setHostname("localhost");
        emailServer.start();
    }

    @Override
    protected void after() {
        emailServer.stop();
    }

    public List<MimeMessage> getMessages() throws MessagingException {
        List<WiserMessage> messages = emailServer.getMessages();
        List<MimeMessage> mimeMessages = new ArrayList<>(messages.size());
        for (WiserMessage message : messages) {
            mimeMessages.add(message.getMimeMessage());
        }
        return mimeMessages;
    }

    public int getPort() {
        return port;
    }
}
