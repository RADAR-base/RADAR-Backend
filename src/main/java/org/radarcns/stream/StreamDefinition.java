/*
 * Copyright 2017 The Hyve and King's College London
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarcns.stream;

import static org.radarcns.stream.AbstractStreamWorker.TIME_WINDOW_COMMIT_INTERVAL_DEFAULT;
import static org.radarcns.util.Comparison.compare;

import java.time.Duration;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.radarcns.topic.KafkaTopic;

public class StreamDefinition implements Comparable<StreamDefinition> {
    private final KafkaTopic inputTopic;
    private final KafkaTopic outputTopic;
    private final TimeWindows window;
    private final Duration commitIntervalMs;

    /**
     * Constructor. It takes in input the topic name to be consumed and to topic name where the
     *      related stream will write the computed values. Default 0 window is used.
     * @param input source {@link KafkaTopic}
     * @param output output {@link KafkaTopic}
     */
    public StreamDefinition(@Nonnull KafkaTopic input, @Nullable KafkaTopic output) {
        this(input, output, (TimeWindows)null, TIME_WINDOW_COMMIT_INTERVAL_DEFAULT);
    }

    /**
     * Constructor. It takes in input the topic name to be consumed and to topic name where the
     *      related stream will write the computed values.
     * @param input source {@link KafkaTopic}
     * @param output output {@link KafkaTopic}
     * @param window time window for aggregation.
     */
    public StreamDefinition(@Nonnull KafkaTopic input, @Nullable KafkaTopic output,
            @Nullable Duration window) {
        this(input, output, window == null ? null : TimeWindows.of(window.toMillis()),
            TIME_WINDOW_COMMIT_INTERVAL_DEFAULT);
    }

    /**
     * Constructor. It takes in input the topic name to be consumed and to topic name where the
     *      related stream will write the computed values.
     * @param input source {@link KafkaTopic}
     * @param output output {@link KafkaTopic}
     * @param window time window for aggregation.
     * @param commitIntervalMs The commit.interval.ms config for the stream
     */
    public StreamDefinition(@Nonnull KafkaTopic input, @Nullable KafkaTopic output,
            @Nullable Duration window, @Nonnull Duration commitIntervalMs) {
        this(input, output, window == null ? null : TimeWindows.of(window.toMillis()),
                commitIntervalMs);
    }


    /**
     * Constructor. It takes in input the topic name to be consumed and to topic name where the
     *      related stream will write the computed values.
     * @param input source {@link KafkaTopic}
     * @param output output {@link KafkaTopic}
     * @param window time window for aggregation.
     * @param commitIntervalMs The commit.interval.ms config for the stream
     */
    public StreamDefinition(@Nonnull KafkaTopic input, @Nullable KafkaTopic output,
            @Nullable TimeWindows window, @Nonnull Duration commitIntervalMs) {
        Objects.requireNonNull(input);
        Objects.requireNonNull(output);

        this.inputTopic = input;
        this.outputTopic = output;
        this.window = window;
        this.commitIntervalMs = commitIntervalMs;
    }

    @Nonnull
    public KafkaTopic getInputTopic() {
        return inputTopic;
    }

    @Nullable
    public KafkaTopic getOutputTopic() {
        return outputTopic;
    }

    /**
     * Kafka Streams allows for stateful stream processing. The internal state is managed in
     *      so-called state stores. A fault-tolerant state store is an internally created and
     *      compacted changelog topic. This function return the changelog topic name.
     *
     * @return {@code String} representing the changelog topic name
     */
    @Nonnull
    public String getStateStoreName() {
        String name = "From-" + getInputTopic().getName();
        if (getOutputTopic() != null) {
            name += "-To-" + getOutputTopic().getName();
        }
        return name;
    }

    @Nullable
    public TimeWindows getTimeWindows() {
        return window;
    }

    @Nonnull
    public Duration getCommitInterval(){
        return commitIntervalMs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StreamDefinition that = (StreamDefinition) o;
        return Objects.equals(inputTopic, that.inputTopic)
                && Objects.equals(outputTopic, that.outputTopic)
                && Objects.equals(window, that.window);
    }

    @Override
    public int hashCode() {
        return Objects.hash(inputTopic, outputTopic, window);
    }

    @Override
    public int compareTo(@Nonnull StreamDefinition o) {
        return compare((StreamDefinition d) -> d.getInputTopic().getName())
                .then(d -> d.getOutputTopic() == null ? "" : d.getOutputTopic().getName())
                .then(d -> d.getTimeWindows() == null ? 0 : d.getTimeWindows().sizeMs)
                .then(d -> d.getTimeWindows() == null ? 0 : d.getTimeWindows().advanceMs)
                .apply(this, o);
    }
}
