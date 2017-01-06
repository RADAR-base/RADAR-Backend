package org.radarcns.stream.aggregator;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.radarcns.config.KafkaProperty;
import org.radarcns.key.MeasurementKey;
import org.radarcns.topic.SensorTopic;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * Runnable abstraction of a Kafka stream that consumes Sensor Topic.
 * @param <V> consumed and aggregated results type
 * @see SensorTopic
 * @see org.radarcns.config.KafkaProperty
 * @see org.radarcns.stream.aggregator.DeviceTimestampExtractor
 */
public abstract class SensorAggregator<V extends SpecificRecord>
        extends AggregatorWorker<MeasurementKey, V, SensorTopic<V>> {
    /**
     * @param topic kafka topic that will be consumed
     * @param clientId useful to debug usign the Kafka log
     * @param master pointer to the MasterAggregator useful to call the notification functions
     */
    public SensorAggregator(@Nonnull SensorTopic<V> topic, @Nonnull String clientId,
                            @Nonnull MasterAggregator master, @Nonnull KafkaProperty kafkaProperty)
            throws IOException {
        this(topic, clientId, 1, master, kafkaProperty);
    }

    /**
     * @param topic kafka topic that will be consumed
     * @param clientId useful to debug usign the Kafka log
     * @param numThread number of threads to execute stream processing
     * @param master pointer to the MasterAggregator useful to call the notification functions
     */
    public SensorAggregator(@Nonnull SensorTopic<V> topic, @Nonnull String clientId,
                            @Nonnull int numThread, @Nonnull MasterAggregator master,
                            @Nonnull KafkaProperty kafkaProperty)
            throws IOException {
        super(topic, clientId, numThread, master, kafkaProperty);
    }

    protected KStreamBuilder getBuilder() throws IOException {
        KStreamBuilder builder = super.getBuilder();

        KStream<MeasurementKey,V> valueKStream = builder.stream(getTopic().getInputTopic());
        setStream(valueKStream, getTopic());

        return builder;
    }

    /**
     * @implSpec it defines the stream computation
     */
    protected abstract void setStream(@Nonnull KStream<MeasurementKey, V> kstream,
                                      @Nonnull SensorTopic<V> topic) throws IOException;
}