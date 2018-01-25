package org.radarcns.util;

import java.io.IOException;
import org.radarcns.kafka.ObservationKey;

/**
 * Store a state for a Kafka consumer. It may not handle maps which have complex objects as keys.
 * Use {@link #keyToString(ObservationKey)} and {@link #stringToKey(String)} to use ObservationKey
 * as a map key by serializing it to String.
 */
public interface PersistentStateStore {
    /** Retrieve a state. The default is returned if no existing state is found.
     *
     * @param groupId Kafka group ID of a consumer or producer.
     * @param clientId Kafka client ID of a consumer or producer.
     * @param stateDefault default state if none is found.
     * @param <T> type of state to retrieve.
     * @throws IOException if the state cannot be deserialized.
     */
    <T> T retrieveState(String groupId, String clientId, T stateDefault) throws IOException;

    /** Store a state.
     * @param groupId Kafka group ID of a consumer or producer.
     * @param clientId Kafka client ID of a consumer or producer.
     * @param value state to store.
     * @throws IOException if the state cannot be serialized or persisted.
     */
    void storeState(String groupId, String clientId, Object value) throws IOException;

    /**
     * Uniquely and efficiently serializes an observation key. It can be deserialized with
     * {@link #stringToKey(String)}.
     * @param key key to serialize
     * @return unique serialized form
     */
    String keyToString(ObservationKey key);

    /**
     * Efficiently serializes an observation key serialized with
     * {@link #keyToString(ObservationKey)}.
     *
     * @param string serialized form
     * @return original measurement key
     */
    ObservationKey stringToKey(String string);
}
