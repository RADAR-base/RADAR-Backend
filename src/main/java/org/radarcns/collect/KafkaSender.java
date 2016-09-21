package org.radarcns.collect;

public interface KafkaSender<K, V> {
    /**
     * Send a message to Kafka eventually.
     */
    void send(String topic, K key, V value);
}
