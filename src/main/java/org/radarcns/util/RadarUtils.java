package org.radarcns.util;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.kstream.Windowed;
import org.radarcns.empaticaE4.EmpaticaE4Acceleration;
import org.radarcns.key.MeasurementKey;
import org.radarcns.key.WindowedKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.primitives.Doubles.asList;

public class RadarUtils {

    private final static Logger log = LoggerFactory.getLogger(RadarUtils.class);

    /**
     * @param record Kafka message of witch you want to know the associated Schema
     * @throws NullPointerException no sensor
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

    public static WindowedKey getWindowed(Windowed<MeasurementKey> window){
        return new WindowedKey(window.key().getUserId(),window.key().getSourceId(),window.window().start(),window.window().end());
    }

    public static double floatToDouble(float input){
        return Double.parseDouble(String.valueOf(input));
    }

    public static double ibiToHR(float input){
        return (60d)/floatToDouble(input);
    }

    public static double[] accelerationToArray(EmpaticaE4Acceleration value){
        return new double[] {
                floatToDouble(value.getX()),
                floatToDouble(value.getY()),
                floatToDouble(value.getY())};
    }

}
