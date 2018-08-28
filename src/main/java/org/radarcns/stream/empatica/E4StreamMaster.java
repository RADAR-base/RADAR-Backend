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
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.stream.StreamMaster;
import org.radarcns.stream.StreamWorker;
import org.radarcns.util.RadarSingletonFactory;

/**
 * Singleton StreamMaster for Empatica E4.
 * @see StreamMaster
 */
public class E4StreamMaster extends StreamMaster {
    @Override
    protected void createWorkers(List<StreamWorker> list, StreamMaster master) {
        RadarPropertyHandler propertyHandler = RadarSingletonFactory.getRadarPropertyHandler();
        list.add(new E4AccelerationStream(highPriority(), master, propertyHandler));
        list.add(new E4BatteryLevelStream(lowPriority(), master, propertyHandler));
        list.add(new E4BloodVolumePulseStream(highPriority(), master, propertyHandler));
        list.add(new E4ElectroDermalActivityStream(normalPriority(), master, propertyHandler));
        list.add(new E4HeartRateStream(lowPriority(), master, propertyHandler));
        list.add(new E4InterBeatIntervalStream(lowPriority(), master, propertyHandler));
        list.add(new E4TemperatureStream(lowPriority(), master, propertyHandler));
    }
}
