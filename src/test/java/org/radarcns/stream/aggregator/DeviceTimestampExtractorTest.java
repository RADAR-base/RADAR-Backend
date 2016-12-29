package org.radarcns.stream.aggregator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
/**
 * Created by nivethika on 20-12-16.
 */
public class DeviceTimestampExtractorTest {

    private DeviceTimestampExtractor timestampExtractor;
    private String topic;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void setUp() {
        this.timestampExtractor = new DeviceTimestampExtractor();

        this.topic = "TESTTopic";
    }

    @Test
    public void extract() {
        String userSchema = "{\"namespace\": \"test.radar.backend\", \"type\": \"record\", "
                +"\"name\": \"TestTimeExtract\","
                +"\"fields\": [{\"name\": \"timeReceived\", \"type\": \"double\"}]}";
        GenericRecord record = buildIndexedRecord(userSchema);
        double timeValue = 40880.051388;
        record.put("timeReceived", timeValue);
        ConsumerRecord<Object, Object> consumerRecord = new ConsumerRecord(topic, 3, 30, null, (IndexedRecord)record);
        long extracted = this.timestampExtractor.extract(consumerRecord);
        assertEquals((long) (1000d * (Double)timeValue ), extracted, 0.0000000);
    }

    @Test
    public void extractWithNotDoubleTimeReceived() {
        String userSchema = "{\"namespace\": \"test.radar.backend\", \"type\": \"record\", "
                +"\"name\": \"TestTimeExtract\","
                +"\"fields\": [{\"name\": \"timeReceived\", \"type\": \"string\"}]}";
        GenericRecord record = buildIndexedRecord(userSchema);
        double timeValue = 40880.051388;
        record.put("timeReceived", "timeValue");
        ConsumerRecord<Object, Object> consumerRecord = new ConsumerRecord(topic, 3, 30, null, (IndexedRecord)record);

        exception.expect(RuntimeException.class);
        exception.expectMessage("Impossible to extract timeReceived from");
        long extracted = this.timestampExtractor.extract(consumerRecord);
        assertNull(extracted);

    }

    private GenericRecord buildIndexedRecord(String userSchema) {

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(userSchema);

        return new GenericData.Record(schema);
    }
}