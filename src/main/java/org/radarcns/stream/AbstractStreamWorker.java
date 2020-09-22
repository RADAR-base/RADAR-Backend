package org.radarcns.stream;

import java.lang.Thread.UncaughtExceptionHandler;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.StreamsException;
import org.radarbase.topic.KafkaTopic;
import org.radarcns.config.ConfigRadar;
import org.radarcns.config.KafkaProperty;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.config.SingleStreamConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractStreamWorker implements StreamWorker, UncaughtExceptionHandler {
    private static final Logger logger = LoggerFactory.getLogger(AbstractStreamWorker.class);

    public static final String OUTPUT_LABEL = "_output";
    public static final Duration TIME_WINDOW_COMMIT_INTERVAL_DEFAULT = Duration.ofSeconds(30);

    private final List<StreamDefinition> streamDefinitions;
    protected SingleStreamConfig config;
    protected ConfigRadar allConfig;
    protected int numThreads;
    protected KafkaProperty kafkaProperty;
    protected StreamMaster master;
    protected List<KafkaStreams> streams;

    public AbstractStreamWorker() {
        streamDefinitions = new ArrayList<>();
    }

    /**
     * Create a stream from input to output topic. By using this method, and {@link
     * #getStreamDefinitions()} automatically gets updated.
     *
     * @param input input topic name
     * @param output output topic name
     */
    protected void defineStream(String input, String output) {
        defineStream(input, output, null);
    }

    /**
     * Create a stream from input to output topic. By using this method, and {@link
     * #getStreamDefinitions()} automatically gets updated.
     *
     * @param input input topic name
     * @param output output topic name
     * @param window time windows size, null if none.
     */
    protected void defineStream(String input, String output, Duration window) {
        streamDefinitions.add(
                new StreamDefinition(new KafkaTopic(input), new KafkaTopic(output), window));
    }

    /**
     * Create a sensor stream from input topic to a "[input]_output" topic. By using this method,
     * and {@link #getStreamDefinitions()} automatically gets updated.
     *
     * @param input input topic name
     */
    protected void defineSensorStream(String input) {
        defineStream(input, input + OUTPUT_LABEL, null);
    }

    /**
     * Create a set of sensor streams, for each of the RADAR standard time frames. An input topic
     * {@code my_input} will create, e.g., {@code my_input_10sec}, {@code my_input_10min} output
     * topics.
     *
     * @param input topic to stream from
     */
    protected void defineWindowedSensorStream(String input) {
        defineWindowedSensorStream(input, input);
    }

    /**
     * Create a set of sensor streams, for each of the RADAR standard time frames. An input topic
     * {@code my_input} with output base {@code my_output} will create, e.g., {@code
     * my_output_10sec}, {@code my_output_10min} output topics.
     *
     * @param input topic to stream from
     * @param outputBase base topic name to stream to
     */
    protected void defineWindowedSensorStream(String input, String outputBase) {
        streamDefinitions.addAll(allConfig.getStream().getTimeWindows(config).stream()
                .map(w -> new StreamDefinition(
                        new KafkaTopic(input),
                        new KafkaTopic(w.getTopicLabel(outputBase)),
                        Duration.ofMillis(w.getIntervalInMilliSec()),
                        allConfig.getStream().getCommitIntervalForTimeWindow(w)
                ))
                .collect(Collectors.toList()));
    }

    @Override
    public Stream<StreamDefinition> getStreamDefinitions() {
        return streamDefinitions.stream();
    }

    @Override
    public void configure(StreamMaster streamMaster, RadarPropertyHandler properties,
            SingleStreamConfig singleConfig) {
        this.kafkaProperty = properties.getKafkaProperties();
        this.allConfig = properties.getRadarProperties();
        this.numThreads = allConfig.getStream().threadsByPriority(singleConfig.getPriority());
        this.config = singleConfig;
        this.master = streamMaster;
        this.initialize();
    }

    /**
     * Starts the stream and notify the StreamMaster.
     */
    @Override
    public void start() {
        if (streams != null) {
            throw new IllegalStateException("Streams already started. Cannot start them again.");
        }
        streams = createStreams();

        if (streams == null) {
            throw new IllegalStateException("Streams are not initialized during doStart");
        }
        streams.forEach(stream -> {
            stream.setUncaughtExceptionHandler(this);
            stream.start();
        });

        master.notifyStartedStream(this);
    }

    protected abstract List<KafkaStreams> createStreams();

    /**
     * Close the stream and notify the StreamMaster.
     */
    @Override
    public void shutdown() {
        logger.info("Shutting down {} stream", getClass().getSimpleName());

        closeStreams();

        master.notifyClosedStream(this);
    }

    protected void closeStreams() {
        if (streams != null) {
            streams.forEach(KafkaStreams::close);
            streams = null;
        }
        doCleanup();
    }

    protected abstract void doCleanup();

    protected abstract void initialize();

    /**
     * Handles exceptions that have been uncaught. It is called when a StreamThread is
     * terminating due to an exception.
     */
    @Override
    public void uncaughtException(Thread t, Throwable e) {
        logger.error("Thread {} has been terminated due to {}", t.getName(), e.getMessage(), e);

        closeStreams();

        if (e instanceof StreamsException) {
            master.restartStream(this);
        } else {
            master.notifyCrashedStream(getClass().getSimpleName());
        }
    }
}
