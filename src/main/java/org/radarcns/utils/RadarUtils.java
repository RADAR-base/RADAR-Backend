package org.radarcns.utils;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;

/**
 * Created by Francesco Nobilia on 04/10/2016.
 */
public class RadarUtils {

    private final static Logger log = Logger.getLogger(RadarUtils.class);

    /**
     * @param record Kafka message of witch you want to know the associated Schema
     * @throws NullPointerException no input
     * @return {key schema, value schema} it might contain null values if no schema has been used
     */
    public static String[] getSchemaName(ConsumerRecord<Object,Object> record){

        if(record == null){
            throw new NullPointerException("Record is null");
        }

        String[] array = new String[2];

        try {
            IndexedRecord value = (IndexedRecord)record.key();
            Schema recordSchema = value.getSchema();
            array[0] = recordSchema.getName();
        }
        catch (ClassCastException e){
            log.error("Key schema cannot be retrieved",e);
        }

        try {
            IndexedRecord value = (IndexedRecord)record.value();
            Schema recordSchema = value.getSchema();
            array[1] = recordSchema.getName();
        }
        catch (ClassCastException e){
            log.error("Value schema cannot be retrieved",e);
        }

        return array;
    }
}
