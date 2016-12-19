package org.radarcns.stream.aggregator;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.radarcns.key.MeasurementKey;
import org.radarcns.topic.sensor.SensorTopic;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * Runnable abstraction of a Kafka stream that consumes Sensor Topic.
 * @param <V> consumed and aggregated results type
 * @see org.radarcns.topic.sensor.SensorTopic
 * @see org.radarcns.config.KafkaProperty
 * @see org.radarcns.stream.aggregator.DeviceTimestampExtractor
 */
public abstract class SensorAggregator<V extends SpecificRecord>
        extends AggregatorWorker<MeasurementKey, V, SensorTopic<V>> {
    /**
     * @param topic: kafka topic that will be consumed
     * @param clientID: useful to debug usign the Kafka log
     * @param master: pointer to the MasterAggregator useful to call the notification functions
     */
    public SensorAggregator(@Nonnull SensorTopic<V> topic, @Nonnull String clientID,
                            @Nonnull MasterAggregator master) throws IOException{
        this(topic, clientID, 1, master);
    }

    /**
     * @param topic: kafka topic that will be consumed
     * @param clientID: useful to debug usign the Kafka log
     * @param numThread: number of threads to execute stream processing
     * @param master: pointer to the MasterAggregator useful to call the notification functions
     */
    public SensorAggregator(@Nonnull SensorTopic<V> topic, @Nonnull String clientID,
                            @Nonnull int numThread, @Nonnull MasterAggregator master)
            throws IOException {
        super(topic, clientID, numThread, master);
    }

    protected KStreamBuilder getBuilder() throws IOException {
        KStreamBuilder builder = new KStreamBuilder();

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