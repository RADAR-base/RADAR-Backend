package org.radarcns.consumer.realtime;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.radarbase.util.RollingTimeAverage;
import org.radarcns.config.ConfigRadar;
import org.radarcns.config.realtime.RealtimeConsumerConfig;
import org.radarcns.monitor.KafkaMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RealtimeInferenceConsumer implements KafkaMonitor {

  private static final Logger logger = LoggerFactory.getLogger(RealtimeInferenceConsumer.class);

  private final Properties properties;
  private final List<Condition> conditions;
  private final List<Action> actions;
  private final String topic;
  private boolean done;
  private Duration pollTimeout;
  private Consumer<GenericRecord, GenericRecord> consumer;

  public RealtimeInferenceConsumer(
      String groupId, String clientId, ConfigRadar radar, RealtimeConsumerConfig consumerConfig) {

    topic = consumerConfig.getTopic();

    if (topic == null || topic.isBlank()) {
      throw new IllegalArgumentException("Cannot start consumer without topic.");
    }

    String consumerClientId = consumerConfig.getName() + "-" + clientId;

    properties = new Properties();
    properties.setProperty(GROUP_ID_CONFIG, groupId);
    properties.setProperty(CLIENT_ID_CONFIG, consumerClientId);
    properties.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "true");

    String deserializer = KafkaAvroDeserializer.class.getName();
    properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, deserializer);
    properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, deserializer);
    properties.setProperty(AUTO_COMMIT_INTERVAL_MS_CONFIG, "1001");
    properties.setProperty(SESSION_TIMEOUT_MS_CONFIG, "15101");
    properties.setProperty(HEARTBEAT_INTERVAL_MS_CONFIG, "7500");

    properties.setProperty(SCHEMA_REGISTRY_URL_CONFIG, radar.getSchemaRegistryPaths());
    properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, radar.getBrokerPaths());

    // Override with any properties specified for this consumer
    properties.putAll(consumerConfig.getConsumerProperties());

    conditions =
        consumerConfig.getConditionConfigs().stream()
            .map(ConditionFactory::getConditionFor)
            .collect(Collectors.toList());

    actions =
        consumerConfig.getActionConfigs().stream()
            .map(ActionFactory::getActionFor)
            .collect(Collectors.toList());

    if (conditions.isEmpty() || actions.isEmpty()) {
      throw new IllegalArgumentException(
          "At least one each of condition and action is necessary to run the consumer.");
    }

    done = false;
    pollTimeout = Duration.ofDays(365);
  }

  @Override
  public void start() throws IOException, InterruptedException {
    consumer = new KafkaConsumer<>(this.properties);
    consumer.subscribe(Collections.singleton(topic));

    logger.info("Consuming realtime inference topic {}", topic);
    RollingTimeAverage ops = new RollingTimeAverage(20000);

    try {
      while (!isShutdown()) {
        try {
          ConsumerRecords<GenericRecord, GenericRecord> records = consumer.poll(getPollTimeout());
          ops.add(records.count());

          for (ConsumerRecord<GenericRecord, GenericRecord> record : records) {
            if (conditions.stream().allMatch(c -> c.isTrueFor(record))) {
              // Only execute the actions if all the conditions are true
              actions.forEach(
                  a -> {
                    try {
                      a.executeFor(record);
                    } catch (IllegalArgumentException | IOException ex) {
                      logger.warn("Error executing action", ex);
                    }
                  });
            }
          }
        } catch (SerializationException ex) {
          logger.warn("Failed to deserialize the record: {}", ex.getMessage());
        } catch (WakeupException ex) {
          logger.info("Consumer woke up");
        } catch (InterruptException ex) {
          logger.info("Consumer was interrupted");
          shutdown();
        } catch (KafkaException ex) {
          logger.error("Kafka consumer gave exception", ex);
        }
      }
    } finally {
      consumer.close();
    }
  }

  @Override
  public void shutdown() throws IOException, InterruptedException {
    logger.info("Shutting down consumer {}", getClass().getSimpleName());
    this.done = true;
    this.consumer.wakeup();
  }

  @Override
  public boolean isShutdown() {
    return done;
  }

  @Override
  public Duration getPollTimeout() {
    return pollTimeout;
  }

  @Override
  public void setPollTimeout(Duration duration) {
    this.pollTimeout = duration;
  }
}
