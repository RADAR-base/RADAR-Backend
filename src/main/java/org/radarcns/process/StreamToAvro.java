package org.radarcns.process;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.List;
import java.util.Properties;

public class StreamToAvro extends KafkaMonitor {
    public StreamToAvro(List<String> topics, String kafkaServers, String schemaUrl) {
        super(topics, kafkaServers, schemaUrl);
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "stream_to_avro");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configure(props);
    }

    protected void evaluateRecords(ConsumerRecords<String, GenericRecord> records) {

    }

    private static class Record {
        String key;
        GenericRecord value;
    }
}