package org.radarbase.util

import org.radarcns.kafka.ObservationKey
import java.io.IOException

/**
 * Store a state for a Kafka consumer. It may not handle maps which have complex objects as keys.
 * Use [keyToString] and [stringToKey] to use ObservationKey
 * as a map key by serializing it to String.
 */
interface PersistentStateStore {
    /** Retrieve a state. The default is returned if no existing state is found.
     *
     * @param groupId Kafka group ID of a consumer or producer.
     * @param clientId Kafka client ID of a consumer or producer.
     * @param stateDefault default state if none is found.
     * @param T type of state to retrieve.
     * @throws IOException if the state cannot be deserialized.
     */
    @Throws(IOException::class)
    fun <T : Any> retrieveState(groupId: String, clientId: String, stateDefault: T): T

    /** Store a state.
     * @param groupId Kafka group ID of a consumer or producer.
     * @param clientId Kafka client ID of a consumer or producer.
     * @param value state to store.
     * @throws IOException if the state cannot be serialized or persisted.
     */
    @Throws(IOException::class)
    fun storeState(groupId: String, clientId: String, value: Any)

    /**
     * Uniquely and efficiently serializes an observation key. It can be deserialized with
     * [stringToKey].
     * @param key key to serialize
     * @return unique serialized form
     */
    fun keyToString(key: ObservationKey): String

    /**
     * Efficiently serializes an observation key serialized with
     * [keyToString].
     *
     * @param string serialized form
     * @return original measurement key
     */
    fun stringToKey(string: String): ObservationKey
}
