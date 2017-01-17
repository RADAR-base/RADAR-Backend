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

package org.radarcns.integration;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.radarcns.data.Record;
import org.radarcns.kafka.AvroTopic;
import org.radarcns.kafka.KafkaSender;
import org.radarcns.kafka.KafkaTopicSender;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * Directly sends a message to Kafka using a KafkaProducer
 */
public class DirectProducer<K, V> implements KafkaSender<K, V> {
    private final KafkaProducer<K, V> producer;

    public DirectProducer(Properties properties) {
        Properties props = new Properties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://localhost:8081");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        if (properties != null) {
            props.putAll(properties);
        }
        producer = new KafkaProducer<>(props);
    }

    @Override
    public <L extends K, W extends V> KafkaTopicSender<L, W> sender(final AvroTopic<L, W> topic) throws IOException {
        return new KafkaTopicSender<L, W>() {
            long lastOffset = -1L;
            @Override
            public void send(long offset, L key, W value) throws IOException {
                if (producer == null) {
                    throw new IllegalStateException("#configure() was not called.");
                }
                producer.send(new ProducerRecord<>(topic.getName(), (K)key, (V)value));

                lastOffset = offset;
            }

            @Override
            public void send(List<Record<L, W>> records) throws IOException {
                if (producer == null) {
                    throw new IllegalStateException("#configure() was not called.");
                }
                for (Record<L, W> record : records) {
                    producer.send(new ProducerRecord<>(topic.getName(), (K)record.key, (V)record.value));
                }
                lastOffset = records.get(records.size() - 1).offset;
            }

            @Override
            public long getLastSentOffset() {
                return lastOffset;
            }

            @Override
            public void clear() {
            }

            @Override
            public void flush() throws IOException {
                producer.flush();
            }

            @Override
            public void close() throws IOException {

            }
        };
    }

    @Override
    public boolean resetConnection() {
        return true;
    }

    @Override
    public boolean isConnected() {
        return true;
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }
}
