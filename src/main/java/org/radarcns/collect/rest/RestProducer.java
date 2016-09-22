package org.radarcns.collect.rest;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonRawValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.http.Consts;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.radarcns.collect.KafkaSender;
import org.radarcns.collect.MockDevice;
import org.radarcns.collect.util.IO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import javafx.util.Pair;


public class RestProducer implements KafkaSender<String, GenericRecord> {
    private final static Logger logger = LoggerFactory.getLogger(RestProducer.class);

    private final String kafkaUrl;
    private final HttpClient httpClient;
    private final Map<String, List<Pair<String, GenericRecord>>> cache;
    private final int batchSize;
    private final EncoderFactory encoderFactory;
    private final static ContentType KAFKA_CONTENT_TYPE = ContentType.create("application/vnd.kafka.avro.v1+json", Consts.UTF_8);
    private final SchemaRegistryClient schemaClient;
    private final Schema.Parser parser;
    private final Map<String, Pair<Integer, Schema>> valueCache;
    private final Map<String, Integer> keyCache;

    /**
     * Create a REST producer that caches some values
     *
     * @param kafkaUrl base Kafka REST service URL.
     * @param schemaUrl base Avro Schema Registry service URL.
     * @param batchSize number of records per topic to batch in one request (set to 0 to disable
     *                  batching)
     */
    public RestProducer(String kafkaUrl, String schemaUrl, int batchSize) {
        this.kafkaUrl = kafkaUrl;
        this.schemaClient = new CachedSchemaRegistryClient(schemaUrl, 1024);
        this.batchSize = batchSize;
        httpClient = HttpClientBuilder.create().build();
        cache = new HashMap<>();
        this.encoderFactory = EncoderFactory.get();
        this.parser = new Schema.Parser();
        this.valueCache = new HashMap<>();
        this.keyCache = new HashMap<>();
    }

    @Override
    public void send(String topic, String key, GenericRecord value) throws IOException {
        List<Pair<String, GenericRecord>> batch = cache.get(topic);

        if (batch == null) {
            batch = new ArrayList<>();
            cache.put(topic, batch);
        } else if (!batch.isEmpty() &&
                !batch.get(0).getValue().getSchema().equals(value.getSchema())) {
            send(topic, batch);
        }
        batch.add(new Pair<>(key, value));

        if (batch.size() + 1 >= this.batchSize) {
            send(topic, batch);
        }
    }

    /**
     * Actually make a REST request to the Kafka REST server and Schema Registry.
     * @param topic Kafka topic
     * @param records values to send
     * @throws IOException if any of the requests fail
     */
    private void send(String topic, List<Pair<String, GenericRecord>> records) throws IOException {
        if (records.isEmpty()) {
            return;
        }
        // Initialize empty Kafka REST proxy request
        KafkaRestRequest request = new KafkaRestRequest();

        // Get schema IDs
        Schema valueSchema = records.get(0).getValue().getSchema();
        Pair<Integer, Schema> cachedValue = valueCache.get(topic);
        if (cachedValue != null && cachedValue.getValue().equals(valueSchema)) {
            request.value_schema_id = cachedValue.getKey();
        } else {
            request.value_schema_id = getSchemaId(topic + "-value", valueSchema);
            if (request.value_schema_id == null) {
                request.value_schema = valueSchema.toString();
            } else {
                valueCache.put(topic, new Pair<>(request.value_schema_id, valueSchema));
            }
        }

        Integer cachedKey = keyCache.get(topic);
        if (cachedKey != null) {
            request.key_schema_id = cachedKey;
        } else {
            request.key_schema_id = getSchemaId(topic + "-key", Schema.create(Schema.Type.STRING));

            if (request.key_schema_id == null) {
                request.key_schema = "\"string\"";
            } else {
                keyCache.put(topic, request.key_schema_id);
            }
        }

        // Encode Avro records
        request.records = new ArrayList<>(records.size());
        for (Pair<String, GenericRecord> recordPair : records) {
            try {
                request.records.add(new Record(recordPair.getKey(), encode(recordPair.getValue())));
            } catch (IOException e) {
                throw new IllegalArgumentException("Cannot encode record", e);
            }
        }

        // Convert request to JSON
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
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

        // Evaluate the result
        String content = IO.readInputStream(postResponse.getEntity().getContent());
        if (postResponse.getStatusLine().getStatusCode() >= 400) {
            System.out.println("FAILED: " + content);
            throw new IOException("Failed to submit: " + content);
        }
        records.clear();
    }

    private Integer getSchemaId(String subject, Schema schema) throws IOException {
        try {
            SchemaMetadata metadata = schemaClient.getLatestSchemaMetadata(subject);
            Schema otherSchema = parser.parse(metadata.getSchema());
            if (otherSchema.equals(schema)) {
                return metadata.getId();
            }
        } catch (RestClientException ex) {
            // try again by registering
        }
        try {
            return schemaClient.register(subject, schema);
        } catch (RestClientException ex1) {
            return null;
        }
    }

    @Override
    public void flush() throws IOException {
        for (Map.Entry<String, List<Pair<String, GenericRecord>>> entry : cache.entrySet()) {
            send(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void close() throws IOException {
        flush();
    }

    private String encode(GenericRecord value) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        JsonEncoder bytes = this.encoderFactory.jsonEncoder(value.getSchema(), out);
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(value.getSchema());

        writer.write(value, bytes);
        bytes.flush();

        byte[] bytes1 = out.toByteArray();
        out.close();
        return new String(bytes1);
    }

    /**
     * Structure of a Kafka REST request to upload data
     */
    private class KafkaRestRequest {
        public String key_schema;
        public String value_schema;
        public Integer key_schema_id;
        public Integer value_schema_id;
        public List<RestProducer.Record> records;
    }

    /**
     * Structure of a single Kafka REST request record
     */
    private static class Record {
        public String key;
        // Use a raw value, so we can put JSON in this String.
        @JsonRawValue
        public String value;
        Record(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }

    public static void main(String[] args) {
        int numberOfDevices = 1;
        if (args.length > 0) {
            numberOfDevices = Integer.parseInt(args[0]);
        }

        logger.info("Simulating the load of " + numberOfDevices);
        MockDevice[] threads = new MockDevice[numberOfDevices];
        KafkaSender<String, GenericRecord>[] senders = new KafkaSender[numberOfDevices];
        for (int i = 0; i < numberOfDevices; i++) {
            senders[i] = new RestProducer("http://ubuntu:8082", "http://ubuntu:8081", 128);
            threads[i] = new MockDevice(senders[i], "device" + i);
            threads[i].start();
        }
        for (MockDevice device : threads) {
            device.waitFor();
        }
    }
}
