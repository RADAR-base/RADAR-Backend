package org.radarcns.collect;

import java.util.List;

public interface KafkaSender<K, V> {
    /**
     * Send a message to Kafka eventually. Returns an offset of the message ID.
     */
    long send(String topic, K key, V value);

    /**
     * Get the latest offsets actually sent for a given topic.
     */
    long getLastSentOffset(String topic);

    /**
     * Flush all remaining messages.
     */
    void flush() throws InterruptedException;

    /**
     * Close the connection.
     */
    void close() throws InterruptedException;
}
