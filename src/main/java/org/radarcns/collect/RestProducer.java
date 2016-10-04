package org.radarcns.collect;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonRawValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.radarcns.ParsedSchemaMetadata;
import org.radarcns.SchemaRetriever;
import org.radarcns.net.HttpClient;
import org.radarcns.net.HttpResponse;
import org.radarcns.util.RollingTimeAverage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Send Avro Records to a Kafka REST Proxy.
 *
 * This queues messages for a specified amount of time and then sends all messages up to that time.
 */
public class RestProducer extends Thread implements KafkaSender<String, GenericRecord> {
    private final static Logger logger = LoggerFactory.getLogger(RestProducer.class);

    private final URL kafkaUrl;
    private final ConcurrentMap<String, List<Record>> cache;
    private final int ageMillis;
    private final EncoderFactory encoderFactory;
    private final SchemaRetriever schemaRetriever;
    private boolean isClosed;
    private final Queue<List<Record>> recordQueue;
    private final static int RETRIES = 3;
    private final static int QUEUE_CAPACITY = 100;
    private final Map<String, Long> lastOffsetsSent;

    /**
     * Create a REST producer that caches some values
     *
     * @param kafkaUrl base Kafka REST service URL.
     * @param schemaRetriever retriever of the registry metadata.
     * @param ageMillis maximum time (ms) that a call for a given topic may aggregate messages.
     */
    public RestProducer(URL kafkaUrl, int ageMillis, SchemaRetriever schemaRetriever) {
        this.kafkaUrl = kafkaUrl;
        this.ageMillis = ageMillis;
        cache = new ConcurrentHashMap<>();
        this.schemaRetriever = schemaRetriever;
        this.encoderFactory = EncoderFactory.get();
        this.isClosed = false;
        this.recordQueue = new ArrayDeque<>(QUEUE_CAPACITY);
        this.lastOffsetsSent = new ConcurrentHashMap<>();
    }

    /**
     * Actually make REST requests.
     *
     * The offsets of the sent messages are added to a
     */
    public void run() {
        List<Record> records;
        RollingTimeAverage opsSent = new RollingTimeAverage(20000L);
        RollingTimeAverage opsRequests = new RollingTimeAverage(20000L);
        try {
            while (true) {
                synchronized (this) {
                    while (!this.isClosed && this.recordQueue.isEmpty()) {
                        wait();
                    }
                    if (this.isClosed) {
                        break;
                    }
                    records = this.recordQueue.element();
                }
                opsSent.add(records.size());
                opsRequests.add(1);

                IOException exception = null;
                for (int i = 0; i < RETRIES; i++) {
                    try {
                        exception = null;
                        send(records);
                        break;
                    } catch (IOException ex) {
                        exception = ex;
                    }
                }
                if (exception == null) {
                    Record lastRecord = records.get(records.size() - 1);
                    lastOffsetsSent.put(lastRecord.topic, lastRecord.offset);
                } else {
                    logger.error("Failed to send records to topic {}: {}", records.get(0).topic, exception.toString());
                }
                synchronized (this) {
                    this.recordQueue.remove();
                    notifyAll();
                }
                logger.debug("Sending {} messages in {} requests per second",
                        (int)Math.round(opsSent.getAverage()),
                        (int)Math.round(opsRequests.getAverage()));
            }
        } catch (InterruptedException e) {
            // exit loop
        }
    }

    /** Enqueue a batch of records for actual sending. */
    private synchronized void enqueue(List<Record> records) {
        if (records.isEmpty()) {
            return;
        }
        recordQueue.add(new ArrayList<>(records));
        logger.debug("Queue size: {}", recordQueue.size());
        notifyAll();
        records.clear();
    }

    /**
     * Send given key and record to a topic.
     * @param topic topic name
     * @param key key
     * @param value value with schema
     */
    @Override
    public void send(long offset, String topic, String key, GenericRecord value) {
        List<Record> batch = cache.get(topic);
        if (batch == null) {
            //noinspection Convert2Diamond
            batch = cache.putIfAbsent(topic, new ArrayList<Record>());
            if (batch == null) {
                batch = cache.get(topic);
            }
        }

        Record record = new Record(offset, topic, key, value);

        synchronized (this) {
            if (!batch.isEmpty() && !batch.get(0).value.getSchema().equals(value.getSchema())) {
                enqueue(batch);
            }
            batch.add(record);

            if (System.currentTimeMillis() - batch.get(0).milliTimeAdded >= this.ageMillis) {
                enqueue(batch);
            }
        }
    }

    @Override
    public long getLastSentOffset(String topic) {
        return this.lastOffsetsSent.get(topic);
    }

    /**
     * Actually make a REST request to the Kafka REST server and Schema Registry.
     * @param records values to send
     * @throws IOException if any of the requests fail
     */
    private void send(List<Record> records) throws IOException {
        if (records.isEmpty()) {
            return;
        }
        // Initialize empty Kafka REST proxy request
        KafkaRestRequest request = new KafkaRestRequest();
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        // Get schema IDs
        Schema valueSchema = records.get(0).value.getSchema();
        String topic = records.get(0).topic;

        ParsedSchemaMetadata metadata = schemaRetriever.getOrSetSchemaMetadata(topic, true, valueSchema);
        if (metadata.getId() != null) {
            request.value_schema_id = metadata.getId();
        } else {
            logger.warn("Cannot get value schema, submitting data to the schema-less topic.");
            request.value_schema = metadata.getSchema().toString();
            topic = "schemaless-value";
        }
        metadata = schemaRetriever.getOrSetSchemaMetadata(topic, false, Schema.create(Schema.Type.STRING));
        if (metadata.getId() != null) {
            request.key_schema_id = metadata.getId();
        } else {
            logger.warn("Cannot get key schema, submitting data to the schema-less topic.");
            request.key_schema = metadata.getSchema().toString();
            topic = "schemaless-key";
        }

        // Encode Avro records
        request.records = new ArrayList<>(records.size());
        for (Record record : records) {
            try {
                String rawValue = encode(record.value);
                request.records.add(new RawRecord(record.key, rawValue));
            } catch (IOException e) {
                throw new IllegalArgumentException("Cannot encode record", e);
            }
        }

        // Convert request to JSON
        String data;
        try {
            data = mapper.writeValueAsString(request);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Cannot encode values", e);
        }
        // Post to Kafka REST server
        HttpResponse response = HttpClient.request(new URL(kafkaUrl, "topics/" + topic), "POST", data);

        // Evaluate the result
        if (response.getStatusCode() < 400) {
            logger.debug("Added message to topic {}: {} -> {}", topic, data, response.getContent());
        } else {
            logger.error("FAILED to transmit message {} -> {}", data, response.getContent());
            throw new IOException("Failed to submit: " + response.getContent());
        }
    }

    @Override
    public void flush() throws InterruptedException {
        synchronized (this) {
            for (List<Record> records : cache.values()) {
                enqueue(records);
            }
            while (!this.isClosed && !this.recordQueue.isEmpty()) {
                wait();
            }
        }
    }

    @Override
    public void close() throws InterruptedException {
        flush();
        synchronized (this) {
            this.isClosed = true;
            this.notifyAll();
        }
        join();
    }

    private String encode(GenericRecord value) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Encoder encoder = this.encoderFactory.jsonEncoder(value.getSchema(), out);
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(value.getSchema());
        writer.write(value, encoder);
        encoder.flush();

        byte[] bytes = out.toByteArray();
        out.close();
        return new String(bytes);
    }

    private class Record {
        final long milliTimeAdded;
        final String topic;
        final String key;
        final GenericRecord value;
        private final long offset;

        Record(long offset, String topic, String key, GenericRecord value) {
            this.offset = offset;
            this.topic = topic;
            this.key = key;
            this.value = value;
            this.milliTimeAdded = System.currentTimeMillis();
        }
    }

    /**
     * Structure of a Kafka REST request to upload data
     */
    private class KafkaRestRequest {
        public String key_schema;
        public String value_schema;
        public Integer key_schema_id;
        public Integer value_schema_id;
        public List<RawRecord> records;
    }

    /**
     * Structure of a single Kafka REST request record
     */
    private static class RawRecord {
        public String key;
        // Use a raw value, so we can put JSON in this String.
        @JsonRawValue
        public String value;
        RawRecord(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }

    public static void main(String[] args) throws InterruptedException, MalformedURLException {
        System.out.println(System.currentTimeMillis());
        int numberOfDevices = 1;
        if (args.length > 0) {
            numberOfDevices = Integer.parseInt(args[0]);
        }

        logger.info("Simulating the load of " + numberOfDevices);
        MockDevice[] threads = new MockDevice[numberOfDevices];
        RestProducer[] senders = new RestProducer[1];
        SchemaRetriever schemaRetriever = new SchemaRegistryRetriever("http://radar-test.thehyve.net:8081");
        SchemaRetriever localSchemaRetriever =  new LocalSchemaRetriever();
        senders[0] = new RestProducer(new URL("http://radar-test.thehyve.net:8082"), 1000, schemaRetriever);
        senders[0].start();
        for (int i = 0; i < numberOfDevices; i++) {
            threads[i] = new MockDevice(senders[0], "device" + i, localSchemaRetriever);
            threads[i].start();
        }
        for (MockDevice device : threads) {
            device.waitFor();
        }
        for (RestProducer sender : senders) {
            sender.close();
        }
    }
}
