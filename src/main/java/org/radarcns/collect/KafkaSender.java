package org.radarcns.collect;

public interface KafkaSender<K, V> {
    /**
     * Send a message to Kafka eventually. Given offset must be strictly monotonically increasing
     * for subsequent calls.
     */
    void send(long offset, String topic, K key, V value);

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
