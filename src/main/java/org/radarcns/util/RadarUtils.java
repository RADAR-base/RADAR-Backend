package org.radarcns.util;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.kstream.Windowed;
import org.radarcns.Statistic;
import org.radarcns.key.MeasurementKey;
import org.radarcns.key.WindowedKey;
import org.radarcns.stream.ValueCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import static org.apache.avro.Schema.parse;

/**
 * Created by Francesco Nobilia on 04/10/2016.
 */
public class RadarUtils {

    private final static Logger log = LoggerFactory.getLogger(RadarUtils.class);

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

    public static WindowedKey getWindowed(Windowed<MeasurementKey> window){
        return new WindowedKey(window.key().getUserId(),window.key().getSourceId(),window.window().start(),window.window().end());
    }

    public static double floatToDouble(float input){
        Float f = new Float(input);
        Double d = new Double(f.toString());
        return d.doubleValue();
    }

    public static double ibiToHR(float input){
        return (60d)/floatToDouble(input);
    }

}
