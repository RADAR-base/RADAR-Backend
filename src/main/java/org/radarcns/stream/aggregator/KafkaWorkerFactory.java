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

package org.radarcns.stream.aggregator;

import org.radarcns.config.RadarBackendOptions;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.empatica.E4Worker;
import org.radarcns.phone.PhoneWorker;

import java.io.IOException;
import java.util.Arrays;

public class KafkaWorkerFactory {
    private final RadarPropertyHandler properties;
    private final RadarBackendOptions options;

    public KafkaWorkerFactory(RadarBackendOptions options,
                              RadarPropertyHandler properties) {
        this.options = options;
        this.properties = properties;
    }

    public MasterAggregator createStreamWorker() throws IOException {
        String[] args = options.getSubCommandArgs();
        String commandType;
        if (args == null || args.length == 0) {
            commandType = "all";
        } else {
            commandType = args[0];
        }
        switch (commandType) {
            case "e4":
                return new E4Worker(properties.getRadarProperties().isStandalone());
            case "phone":
                return new PhoneWorker(properties.getRadarProperties().isStandalone());
            case "all":
                return new CombinedKafkaWorker(Arrays.asList(
                        new E4Worker(properties.getRadarProperties().isStandalone()),
                        new PhoneWorker(properties.getRadarProperties().isStandalone())
                        ));
            default:
                throw new IllegalArgumentException("Cannot create unknown monitor " + commandType);
        }
    }
}
