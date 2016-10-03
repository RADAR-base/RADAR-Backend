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
import org.apache.http.Consts;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.radarcns.ParsedSchemaMetadata;
import org.radarcns.SchemaRetriever;
import org.radarcns.util.IO;
import org.radarcns.util.RollingTimeAverage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Send Avro Records to a Kafka REST Proxy.
 *
 * This queues messages for a specified amount of time and then sends all messages up to that time.
 */
public class RestProducer extends Thread implements KafkaSender<String, GenericRecord> {
    private final static Logger logger = LoggerFactory.getLogger(RestProducer.class);

    private final String kafkaUrl;
    private final HttpClient httpClient;
    private final ConcurrentMap<String, List<Record>> cache;
    private final int ageMillis;
    private final EncoderFactory encoderFactory;
    private final static ContentType KAFKA_CONTENT_TYPE = ContentType.create("application/vnd.kafka.avro.v1+json", Consts.UTF_8);
    private final SchemaRetriever schemaRetriever;
    private boolean isClosed;
    private final Queue<List<Record>> recordQueue;
    private final static int RETRIES = 3;
    private final static int QUEUE_CAPACITY = 100;
    private final Map<String, Long> lastOffsetsSent;
    private final AtomicLong currentOffset;

    /**
     * Create a REST producer that caches some values
     *
     * @param kafkaUrl base Kafka REST service URL.
     * @param schemaRetriever retriever of the registry metadata.
     * @param ageMillis maximum time (ms) that a call for a given topic may aggregate messages.
     */
    public RestProducer(String kafkaUrl, int ageMillis, SchemaRetriever schemaRetriever) {
        this.kafkaUrl = kafkaUrl;
        this.ageMillis = ageMillis;
        httpClient = HttpClientBuilder.create().build();
        cache = new ConcurrentHashMap<>();
        this.schemaRetriever = schemaRetriever;
        this.encoderFactory = EncoderFactory.get();
        this.isClosed = false;
        this.recordQueue = new ArrayDeque<>(QUEUE_CAPACITY);
        this.lastOffsetsSent = new ConcurrentHashMap<>();
        this.currentOffset = new AtomicLong(1L);
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
                    logger.error("Failed to send records to topic " + records.get(0).topic, exception);
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
    public long send(String topic, String key, GenericRecord value) {
        List<Record> batch = cache.get(topic);
        if (batch == null) {
            batch = cache.putIfAbsent(topic, new ArrayList<Record>());
            if (batch == null) {
                batch = cache.get(topic);
            }
        }

        Record record = new Record(this.currentOffset.getAndIncrement(), topic, key, value);

        synchronized (this) {
            if (!batch.isEmpty() && !batch.get(0).value.getSchema().equals(value.getSchema())) {
                enqueue(batch);
            }
            batch.add(record);

            if (System.currentTimeMillis() - batch.get(0).milliTimeAdded >= this.ageMillis) {
                enqueue(batch);
            }
        }

        return record.offset;
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
            request.value_schema = mapper.writeValueAsString(metadata.getSchema());
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
        HttpPost post = new HttpPost(kafkaUrl + "/topics/" + topic);
        post.setEntity(new StringEntity(data, KAFKA_CONTENT_TYPE));
        post.setHeader(new BasicHeader("Accept", "application/vnd.kafka.v1+json, application/vnd.kafka+json, application/json"));
        HttpResponse postResponse = httpClient.execute(post);

        String content = IO.readInputStream(postResponse.getEntity().getContent());

        // Evaluate the result
        if (postResponse.getStatusLine().getStatusCode() < 400) {
            logger.debug("Added message to topic {}: {} -> {}", topic, data, content);
        } else {
            logger.error("FAILED to transmit message {} -> {}", data, content);
            throw new IOException("Failed to submit: " + content);
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

    public static void main(String[] args) throws InterruptedException {
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
        senders[0] = new RestProducer("http://radar-test.thehyve.net:8082", 1000, schemaRetriever);
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
