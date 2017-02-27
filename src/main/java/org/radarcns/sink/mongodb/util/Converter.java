package org.radarcns.sink.mongodb.util;

import org.apache.kafka.connect.data.Struct;
import org.bson.BsonDateTime;

/**
 * Generic converter for MongoDB.
 */
public final class Converter {

    /**
     * Private constructor to prevent instantiation.
     */
    private Converter() {}

    /**
     * Try to convert to BsonDateTime.
     *
     * @param obj raw value
     * @return BsonDateTime, or null if unsuccessful
     */
    public static BsonDateTime toDateTime(Object obj) {
        if (obj instanceof Long) {
            return new BsonDateTime((Long)obj);
        } else if (obj instanceof Double) {
            return new BsonDateTime((long)(1000d * (Double) obj));
        } else {
            return null;
        }
    }

    /**
     * Creates a key string using userId and sourceId.
     *
     * @param key
     * @return converted key string
     */
    public static String measurementKeyToMongoDbKey(Struct key) {
        return key.get("userId") + "-" + key.get("sourceId");
    }
}
