package org.radarcns.stream.aggregator;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.radarcns.config.KafkaProperty;
import org.radarcns.key.MeasurementKey;
import org.radarcns.topic.sensor.SensorTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.Nonnull;

/**
 * Runnable abstraction of a Kafka stream that consumes Sensor Topic.
 * @param <V> consumed and aggregated results type
 * @see org.radarcns.topic.sensor.SensorTopic
 * @see org.radarcns.config.KafkaProperty
 * @see org.radarcns.stream.aggregator.DeviceTimestampExtractor
 */
public abstract class SensorAggregator<V extends SpecificRecord> implements AggregatorWorker {
    private static final Logger log = LoggerFactory.getLogger(SensorAggregator.class);

    private final String clientID;
    private final KafkaStreams streams;
    private final SensorTopic<V> topic;

    private final MasterAggregator master;

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
        if (numThread < 1) {
            throw new IllegalStateException(
                    "The number of concurrent threads must be bigger than 0");
        }

        this.topic = topic;
        this.clientID = clientID;
        this.master = master;

        streams = new KafkaStreams(getBuilder(),
                KafkaProperty.getStream(clientID, numThread, DeviceTimestampExtractor.class));

        streams.setUncaughtExceptionHandler(this);

        log.info("Creating {} stream", clientID);
    }

    /**
     * @return KStreamBuilder used to instantiate the Kafka Streams
     */
    private KStreamBuilder getBuilder() throws IOException{
        KStreamBuilder builder = new KStreamBuilder();

        log.trace(topic.getInputTopic());

        KStream<MeasurementKey, V> valueKStream =  builder.stream(topic.getInputTopic());

        setStream(valueKStream, topic);

        log.info("Creating the builder for {} stream", clientID);

        return builder;
    }

    /**
     * @implSpec it defines the stream computation
     */
    protected abstract void setStream(@Nonnull KStream<MeasurementKey, V> kstream,
                                      @Nonnull SensorTopic<V> topic) throws IOException;

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
    @Override
    public void shutdown(){
        log.info("Shutting down {} stream", clientID);
        streams.close();

        master.notifyClosedStream(clientID);
    }

    /**
     * @return the streams' client ID
     */
    @Override
    public String getClientID(){
        return this.clientID;
    }

    /**
     * @return the streams' name
     */
    @Override
    public String getName(){
        return getClientID();
    }

    /**
     * @return a Thread ready to run the current instance of SensorAggregator
     */
    @Override
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
}

