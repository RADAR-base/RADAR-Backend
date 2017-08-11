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

import org.radarcns.config.ConfigRadar;
import org.radarcns.config.RadarBackendOptions;
import org.radarcns.config.RadarPropertyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class KafkaStreamFactory {
    private static final Logger logger = LoggerFactory.getLogger(
            KafkaStreamFactory.class.getName());

    private final ConfigRadar config;
    private final RadarBackendOptions options;

    public KafkaStreamFactory(RadarBackendOptions options,
                              RadarPropertyHandler properties) {
        this.options = options;
        this.config = properties.getRadarProperties();
    }

    public StreamMaster createStreamWorker() {
        List<String> streamTypes;

        // Try to get the stream type from the commandline arguments
        String[] args = options.getSubCommandArgs();
        if (args != null && args.length > 0) {
            streamTypes = Arrays.asList(args);
        } else {
            streamTypes = config.getStreamMasters();
        }

        List<StreamMaster> masters = new ArrayList<>(streamTypes.size());

        for (String streamType : streamTypes) {
            try {
                masters.add((StreamMaster) Class.forName(streamType).newInstance());
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                logger.error("Cannot instantiate StreamMaster class {}", streamType, e);
            } catch (ClassCastException e) {
                logger.error("Given type {} is not a StreamMaster", streamType);
            }
        }

        StreamMaster master;

        if (masters.isEmpty()) {
            throw new IllegalArgumentException("No StreamMasters specified");
        } else if (masters.size() == 1) {
            master = masters.get(0);
        } else {
            master = new CombinedStreamMaster(masters);
        }

        master.setNumberOfThreads(config);
        return master;
    }
}
