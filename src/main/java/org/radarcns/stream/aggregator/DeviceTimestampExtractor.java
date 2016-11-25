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
 * Created by Francesco Nobilia on 24/11/2016.
 */
public class DeviceTimestampExtractor implements TimestampExtractor {

    private final static Logger log = LoggerFactory.getLogger(DeviceTimestampExtractor.class);

    /*private long testInit = -1;
    private long testLast = -1;*/

    @Override
    public long extract(ConsumerRecord<Object, Object> record) {

        IndexedRecord value = (IndexedRecord)record.value();
        Schema recordSchema = value.getSchema();

        try{
        Schema.Field field = recordSchema.getField("timeReceived");

            if(value.get(field.pos()) instanceof Double){

                /*if(testInit == -1){
                    testInit = RadarUtils.doubleToLong((Double) value.get(field.pos()));
                }
                testLast = RadarUtils.doubleToLong((Double) value.get(field.pos()));

                Date dateInit = new Date((new Timestamp(testInit)).getTime());
                Date dateEnd = new Date((new Timestamp(testLast)).getTime());

                log.info("Init is {} and last seen is {}",dateInit.toString(),dateEnd.toString());*/

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

