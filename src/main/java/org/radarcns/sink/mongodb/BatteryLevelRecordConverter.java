package org.radarcns.sink.mongodb;

import java.util.Collection;
import java.util.Collections;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.Document;
import org.radarcns.serialization.RecordConverter;

/**
 * RecordConverter to convert a BatteryLevel record to a MongoDB Document
 */
public class BatteryLevelRecordConverter implements RecordConverter {

    /**
     * Returns the list of supported schemas, which behaves as the id to select suitable
     * RecordConverter for a SinkRecord
     *
     * @return a list of supported Schemas
     */
    @Override
    public Collection<String> supportedSchemaNames() {
        return Collections.singleton("org.radarcns.key.MeasurementKey-"
                + "org.radarcns.empatica.EmpaticaE4BatteryLevel");
    }

    /**
     * Converts a BatteryLevel SinkRecord into a MongoDB Document
     *
     * @param sinkRecord record to be converted
     * @return converted MongoDB Document to write
     */
    @Override
    public Document convert(SinkRecord sinkRecord) throws DataException {

        Struct key = (Struct) sinkRecord.key();
        Struct value = (Struct) sinkRecord.value();

        return new Document("_id", measurementKeyToMongoDbKey(key))
                .append("user", key.getString("userId"))
                .append("source", key.getString("sourceId"))
                .append("batteryLevel", value.getFloat32("batteryLevel"))
                .append("timeReceived", value.getFloat64("timeReceived"));
    }


    /**
     * Creates a key string using userId and sourceId
     * @param key
     * @return converted key string
     */
    private static String measurementKeyToMongoDbKey(Struct key) {
        return key.get("userId")
                + "-" + key.get("sourceId");
    }
}
