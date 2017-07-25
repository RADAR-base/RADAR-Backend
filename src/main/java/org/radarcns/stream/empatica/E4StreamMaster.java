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

package org.radarcns.stream.empatica;

import java.util.Arrays;
import java.util.List;
import org.radarcns.config.KafkaProperty;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.stream.StreamGroup;
import org.radarcns.stream.StreamMaster;
import org.radarcns.stream.StreamWorker;
import org.radarcns.util.RadarSingletonFactory;

/**
 * Singleton StreamMaster for Empatica E4.
 * @see StreamMaster
 */
public class E4StreamMaster extends StreamMaster {

    public E4StreamMaster(boolean standalone) {
        super(standalone,"Empatica E4");
    }

    protected StreamGroup getStreamGroup() {
        return  E4Streams.getInstance();
    }

    @Override
    protected List<StreamWorker<?,?>> createWorkers(int low, int normal, int high) {
        RadarPropertyHandler propertyHandler = RadarSingletonFactory.getRadarPropertyHandler();
        KafkaProperty kafkaProperty = propertyHandler.getKafkaProperties();
        return Arrays.asList(
                new E4AccelerationStream(
                        "E4AccelerationStream", high, this, kafkaProperty),
                new E4BatteryLevelStream(
                        "E4BatteryLevelStream", low, this, kafkaProperty),
                new E4BloodVolumePulseStream(
                        "E4BloodVolumePulseStream", high, this, kafkaProperty),
                new E4ElectroDermalActivityStream(
                        "E4ElectroDermalActivityStream", normal, this, kafkaProperty),
                new E4HeartRateStream(
                        "E4HeartRateStream", high,this, kafkaProperty),
                new E4InterBeatIntervalStream(
                        "E4InterBeatIntervalStream", high,this, kafkaProperty),
                new E4TemperatureStream(
                        "E4TemperatureStream", high, this, kafkaProperty));
    }
}
