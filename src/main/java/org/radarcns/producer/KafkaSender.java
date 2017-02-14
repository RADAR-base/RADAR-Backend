/*
 * Copyright 2017 Kings College London and The Hyve
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

package org.radarcns.producer;

import java.io.Closeable;
import java.io.IOException;
import org.radarcns.topic.AvroTopic;

/** Thread-safe sender */
public interface KafkaSender<K, V> extends Closeable {
    /** Get a non thread-safe sender instance. */
    <L extends K, W extends V> KafkaTopicSender<L, W> sender(AvroTopic<L, W> topic)
            throws IOException;

    /**
     * If the sender is no longer connected, try to reconnect.
     * @return whether the connection has been restored.
     */
    boolean resetConnection();

    /**
     * Whether the sender is connected to the Kafka system.
     */
    boolean isConnected();
}