package org.radarcns.stream.aggregator;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.radarcns.util.RadarUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom TimestampExtractor for TimeWindows Streams.
 */
public class DeviceTimestampExtractor implements TimestampExtractor {

    private final static Logger log = LoggerFactory.getLogger(DeviceTimestampExtractor.class);

    /**
     * Return the timeReceived value converted in long.
     * timeReceived is the timestamp at which the device has collected the sample.
     * @throws RuntimeException if timeReceived is not present inside the analysed record
     */
    @Override
    public long extract(ConsumerRecord<Object, Object> record) {

        IndexedRecord value = (IndexedRecord)record.value();
        Schema recordSchema = value.getSchema();

        try{
        Schema.Field field = recordSchema.getField("timeReceived");

            if(value.get(field.pos()) instanceof Double){
                return RadarUtils.doubleToLong((Double) value.get(field.pos()));
            }
            else{
                log.error("timeReceived id not a Double in {}",record.toString());
            }

        }
        catch (AvroRuntimeException e){
            log.error("Cannot extract timeReceived form {}",record.toString(),e);
        }

        throw new RuntimeException("Impossible to extract timeReceived from "+record.toString());
    }
}

