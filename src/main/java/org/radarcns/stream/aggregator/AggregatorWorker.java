package org.radarcns.stream.aggregator;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.radarcns.config.KafkaProperty;
import org.radarcns.topic.AvroTopic;
import org.radarcns.util.RadarSingletonFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * Runnable abstraction of Kafka Stream Handler
 */
public abstract class AggregatorWorker<K extends SpecificRecord, V extends SpecificRecord,
        T extends AvroTopic<K, V>> implements Runnable, Thread.UncaughtExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(AggregatorWorker.class);

    private final String clientID;
    private final KafkaStreams streams;
    private final MasterAggregator master;
    private KafkaProperty kafkaProperty =
            RadarSingletonFactory.getRadarPropertyHandler().getKafkaProperties();

    private final T topic;

    public AggregatorWorker(@Nonnull T topic, @Nonnull String clientID, int numThreads,
                            @Nonnull MasterAggregator aggregator) throws IOException {
        if (numThreads < 1) {
            throw new IllegalStateException(
                    "The number of concurrent threads must be at least 1");
        }

        this.clientID = clientID;
        this.master = aggregator;
        this.topic = topic;
        log.info("Creating the stream {} from topic {} to topic {}",
                getClientID(), getTopic().getInputTopic(), getTopic().getOutputTopic());

        this.streams = new KafkaStreams(getBuilder(),
                kafkaProperty.getStream(getClientID(), numThreads, DeviceTimestampExtractor.class));
        this.streams.setUncaughtExceptionHandler(this);
    }

    /** Create a Kafka Stream builder */
    protected abstract KStreamBuilder getBuilder() throws IOException;

    public String getName() {
        return clientID;
    }


    /**
     * It starts the stream and notify the MasterAggregator
     */
    @Override
    public void run() {
        log.info("Starting {} stream", clientID);
        streams.start();

        master.notifyStartedStream(clientID);
    }

    /**
     * It closes the stream and notify the MasterAggregator
     */
    public void shutdown() {
        log.info("Shutting down {} stream", getClientID());
        streams.close();

        master.notifyClosedStream(clientID);
    }

    public String getClientID() {
        return clientID;
    }

    /**
     * @return a Thread ready to run the current instance of AggregatorWorker
     */
    public Thread getThread() {
        Thread thread;

        thread = new Thread(this);
        thread.setName(this.clientID);

        return thread;
    }

    /**
     * It handles exceptions that have been uncaught. It is called when a StreamThread is
     * terminating due to an exception.
     */
    @Override
    public void uncaughtException(Thread t, Throwable e) {
        log.error("Thread {} has been terminated due to {}", t.getName(), e.getMessage(), e);

        master.notifyCrashedStream(clientID);

        //TODO find a better solution based on the exception
    }

    protected KafkaStreams getStreams() {
        return streams;
    }

    protected T getTopic() {
        return topic;
    }

    protected void setKafkaProperty(KafkaProperty kafkaProperty) {
        this.kafkaProperty = kafkaProperty;
    }
}
