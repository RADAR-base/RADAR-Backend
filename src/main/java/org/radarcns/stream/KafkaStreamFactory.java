/*
 * Copyright 2017 King's College London and The Hyve
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

import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.radarcns.config.RadarBackendOptions;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.config.SingleStreamConfig;
import org.radarcns.config.SourceStatisticsStreamConfig;
import org.radarcns.config.SubCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaStreamFactory {
    private static final Logger logger = LoggerFactory.getLogger(
            KafkaStreamFactory.class.getName());

    private final RadarPropertyHandler radarProperties;
    private final RadarBackendOptions options;

    public KafkaStreamFactory(RadarBackendOptions options,
                              RadarPropertyHandler properties) {
        this.options = options;
        this.radarProperties = properties;
    }

    public StreamMaster createSensorStreams() {
        List<String> args = options.getSubCommandArgs();

        Stream<SingleStreamConfig> configStream = radarProperties.getRadarProperties()
                .getStream().getStreamConfigs();

        if (!args.isEmpty()) {
            Set<String> streamTypes = args.stream()
                    .map(s -> s.toLowerCase(Locale.US))
                    .collect(Collectors.toSet());

            configStream = configStream.filter(s -> {
                String clsName = s.getStreamClass().getName().toLowerCase(Locale.US);
                return streamTypes.stream().anyMatch(clsName::endsWith);
            });
        }

        return master(configStream);
    }

    private StreamMaster master(Stream<? extends SingleStreamConfig> configs) {
        return new StreamMaster(radarProperties, configs);
    }

    public SubCommand createStreamStatistics() {
        List<SourceStatisticsStreamConfig> configs = radarProperties.getRadarProperties()
                .getStream().getSourceStatistics();

        if (configs == null) {
            logger.warn("Statistics monitor is not configured. Cannot start it.");
            return master(Stream.empty());
        }

        return master(configs.stream());
    }
}
