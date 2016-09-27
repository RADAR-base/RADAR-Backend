package org.radarcns.collect;

import java.io.IOException;

public interface KafkaSender<K, V> {
    /**
     * Send a message to Kafka eventually.
     */
    void send(String topic, K key, V value);

    /**
     * Flush all remaining messages.
     */
    void flush() throws InterruptedException;

    /**
     * Close the connection.
     */
    void close() throws InterruptedException;
}
