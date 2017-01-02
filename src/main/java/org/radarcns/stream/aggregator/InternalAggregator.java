package org.radarcns.stream.aggregator;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.radarcns.config.KafkaProperty;
import org.radarcns.key.MeasurementKey;
import org.radarcns.key.WindowedKey;
import org.radarcns.topic.InternalTopic;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * Runnable abstraction of a Kafka stream that consumes Internal Topic
 * @param <I> consumed message type
 * @param <O> aggregated results type
 * @see InternalTopic
 * @see org.radarcns.config.KafkaProperty
 * @see org.radarcns.stream.aggregator.DeviceTimestampExtractor
 */
public abstract class InternalAggregator<I, O extends SpecificRecord>
        extends AggregatorWorker<WindowedKey, O, InternalTopic<O>> {
    /**
     * @param topic:    kafka topic that will be consumed
     * @param clientID: useful to debug usign the Kafka log
     * @param master:   pointer to the MasterAggregator useful to call the notification functions
     */
    public InternalAggregator(@Nonnull InternalTopic<O> topic, @Nonnull String clientID,
                              @Nonnull MasterAggregator master, @Nonnull KafkaProperty kafkaProperty) throws IOException {
        this(topic, clientID, 1, master, kafkaProperty);
    }

    /**
     * @param topic:     kafka topic that will be consumed
     * @param clientID:  useful to debug usign the Kafka log
     * @param numThread: number of threads to execute stream processing
     * @param master:    pointer to the MasterAggregator useful to call the notification functions
     */
    public InternalAggregator(@Nonnull InternalTopic<O> topic, @Nonnull String clientID,
                              int numThread, @Nonnull MasterAggregator master,@Nonnull KafkaProperty kafkaProperty )
            throws IOException {
        super(topic, clientID, numThread, master, kafkaProperty);
    }

    @Override
    protected KStreamBuilder getBuilder() throws IOException {
        KStreamBuilder builder = super.getBuilder();

        KStream<MeasurementKey, I> valueKStream = builder.stream(getTopic().getInputTopic());
        setStream(valueKStream, getTopic());

        return builder;
    }

    /**
     * @implSpec it defines the stream computation
     */
    protected  abstract void setStream(@Nonnull KStream<MeasurementKey, I> kstream,
                                      @Nonnull InternalTopic<O> topic) throws IOException;

}
