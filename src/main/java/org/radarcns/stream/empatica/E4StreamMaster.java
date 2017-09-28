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

    protected StreamGroup getStreamGroup() {
        return  E4Streams.getInstance();
    }

    @Override
    protected void createWorkers(List<StreamWorker<?, ?>> list, StreamMaster master) {
        RadarPropertyHandler propertyHandler = RadarSingletonFactory.getRadarPropertyHandler();
        KafkaProperty kafkaProperty = propertyHandler.getKafkaProperties();
        E4Streams defs = E4Streams.getInstance();
        list.add(new E4AccelerationStream(
                defs.getAccelerationStream(), highPriority(), master, kafkaProperty));
        list.add(new E4BatteryLevelStream(
                defs.getBatteryLevelStream(), lowPriority(), master, kafkaProperty));
        list.add(new E4BloodVolumePulseStream(
                defs.getBloodVolumePulseStream(), highPriority(), master, kafkaProperty));
        list.add(new E4ElectroDermalActivityStream(
                defs.getElectroDermalActivityStream(), normalPriority(), master, kafkaProperty));
        list.add(new E4HeartRateStream(
                defs.getHeartRateStream(), lowPriority(), master, kafkaProperty));
        list.add(new E4InterBeatIntervalStream(
                defs.getInterBeatIntervalStream(), lowPriority(), master, kafkaProperty));
        list.add(new E4TemperatureStream(
                defs.getTemperatureStream(), lowPriority(), master, kafkaProperty));
    }
}
