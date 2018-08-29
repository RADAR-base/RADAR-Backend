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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
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
        Collection<String> streamTypes;

        String[] args = options.getSubCommandArgs();
        if (args != null && args.length > 0) {
            streamTypes = new HashSet<>(Arrays.asList(args));
        } else {
            streamTypes = Collections.emptySet();
        }

        return master(radarProperties.getRadarProperties().getStream().getStreamConfigs().stream()
                .filter(s -> streamTypes.isEmpty() || streamTypes.stream().anyMatch(n ->
                        s.getStreamClass().getName().toLowerCase(Locale.US)
                                .endsWith(n.toLowerCase(Locale.US)))));
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
