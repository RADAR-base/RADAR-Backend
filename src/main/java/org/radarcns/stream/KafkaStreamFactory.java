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
import org.radarcns.config.RadarBackendOptions;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.stream.empatica.E4StreamMaster;
import org.radarcns.stream.phone.PhoneStreamMaster;

public class KafkaStreamFactory {
    private final RadarPropertyHandler properties;
    private final RadarBackendOptions options;

    public KafkaStreamFactory(RadarBackendOptions options,
                              RadarPropertyHandler properties) {
        this.options = options;
        this.properties = properties;
    }

    public StreamMaster createStreamWorker() {
        String streamType = properties.getRadarProperties().getStreamWorker();
        if (streamType == null) {
            // Try to get the stream type from the commandline arguments
            String[] args = options.getSubCommandArgs();
            if (args == null || args.length == 0) {
                streamType = "all";
            } else {
                streamType = args[0];
            }
        }

        boolean isStandalone = properties.getRadarProperties().isStandalone();
        switch (streamType) {
            case "e4":
                return new E4StreamMaster(isStandalone);
            case "phone":
                return new PhoneStreamMaster(isStandalone);
            case "all":
                return new CombinedStreamMaster(Arrays.asList(
                        new E4StreamMaster(isStandalone),
                        new PhoneStreamMaster(isStandalone)
                        ));
            default:
                throw new IllegalArgumentException("Cannot create unknown stream " + streamType);
        }
    }
}
