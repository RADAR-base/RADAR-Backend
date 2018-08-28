package org.radarcns.stream;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.radarcns.topic.KafkaTopic;

public abstract class AbstractStreamWorker implements StreamWorker {
  public static final String OUTPUT_LABEL = "_output";
  private static final Map<TimeWindowMetadata, Duration> TIME_WINDOW_COMMIT_INTERVALS =
      new EnumMap<>(TimeWindowMetadata.class);
  static {
    TIME_WINDOW_COMMIT_INTERVALS.put(TimeWindowMetadata.TEN_SECOND, Duration.ofSeconds(10));
    TIME_WINDOW_COMMIT_INTERVALS.put(TimeWindowMetadata.ONE_MIN, Duration.ofSeconds(30));
    TIME_WINDOW_COMMIT_INTERVALS.put(TimeWindowMetadata.TEN_MIN, Duration.ofMinutes(5));
    TIME_WINDOW_COMMIT_INTERVALS.put(TimeWindowMetadata.ONE_HOUR, Duration.ofMinutes(30));
    TIME_WINDOW_COMMIT_INTERVALS.put(TimeWindowMetadata.ONE_DAY, Duration.ofHours(2));
    TIME_WINDOW_COMMIT_INTERVALS.put(TimeWindowMetadata.ONE_WEEK, Duration.ofHours(3));
  }
  public static final Duration TIME_WINDOW_COMMIT_INTERVAL_DEFAULT = Duration.ofSeconds(30);


  private final List<StreamDefinition> streamDefinitions;

  public AbstractStreamWorker() {
    streamDefinitions = new ArrayList<>();
  }

  /**
   * Create a stream from input to output topic. By using this method,
   * and {@link #getStreamDefinitions()} automatically gets updated.
   * @param input input topic name
   * @param output output topic name
   */
  protected void createStream(String input, String output) {
    createStream(input, output, null);
  }

  /**
   * Create a stream from input to output topic. By using this method,
   * and {@link #getStreamDefinitions()} automatically gets updated.
   * @param input input topic name
   * @param output output topic name
   * @param window time windows size, null if none.
   */
  protected void createStream(String input, String output, Duration window) {
    streamDefinitions.add(
        new StreamDefinition(new KafkaTopic(input), new KafkaTopic(output), window));
  }

  /**
   * Create a sensor stream from input topic to a "[input]_output" topic. By using this method,
   * and {@link #getStreamDefinitions()} automatically gets updated.
   * @param input input topic name
   */
  protected void createSensorStream(String input) {
    createStream(input, input + OUTPUT_LABEL, null);
  }

  /**
   * Create a set of sensor streams, for each of the RADAR standard time frames. An input topic
   * {@code my_input} will create, e.g., {@code my_input_10sec}, {@code my_input_10min} output
   * topics.
   * @param input topic to stream from
   */
  protected void createWindowedSensorStream(String input) {
    createWindowedSensorStream(input, input);
  }


  /**
   * Create a set of sensor streams, for each of the RADAR standard time frames. An input topic
   * {@code my_input} with output base {@code my_output} will create, e.g.,
   * {@code my_output_10sec}, {@code my_output_10min} output topics.
   * @param input topic to stream from
   * @param outputBase base topic name to stream to
   */
  protected void createWindowedSensorStream(String input,
      String outputBase) {
    streamDefinitions.addAll(Arrays.stream(TimeWindowMetadata.values())
          .map(w -> new StreamDefinition(
                new KafkaTopic(input),
                new KafkaTopic(w.getTopicLabel(outputBase)),
                Duration.ofMillis(w.getIntervalInMilliSec()),
                getCommitIntervalForTimeWindow(w)))
          .collect(Collectors.toList()));
  }

  public static Duration getCommitIntervalForTimeWindow(TimeWindowMetadata metadata) {
    return TIME_WINDOW_COMMIT_INTERVALS.getOrDefault(metadata, TIME_WINDOW_COMMIT_INTERVAL_DEFAULT);
  }

  @Override
  public Stream<StreamDefinition> getStreamDefinitions() {
    return streamDefinitions.stream();
  }
}
