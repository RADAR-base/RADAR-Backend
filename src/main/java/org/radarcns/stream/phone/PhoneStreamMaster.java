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

package org.radarcns.stream.phone;

import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.stream.StreamGroup;
import org.radarcns.stream.StreamMaster;
import org.radarcns.stream.StreamWorker;
import org.radarcns.util.RadarSingletonFactory;

import java.util.List;

/**
 * Singleton StreamMaster for Phone
 * @see StreamMaster
 */
public class PhoneStreamMaster extends StreamMaster {
    @Override
    protected StreamGroup getStreamGroup() {
        return PhoneStreams.getInstance();
    }

    @Override
    protected void createWorkers(List<StreamWorker<?, ?>> list, StreamMaster master) {
        RadarPropertyHandler propertyHandler = RadarSingletonFactory.getRadarPropertyHandler();
        PhoneStreams defs = PhoneStreams.getInstance();
        list.add(new PhoneUsageStream(
                defs.getUsageStream(), lowPriority(), master, propertyHandler));
        list.add(new PhoneUsageAggregationStream(
                defs.getUsageEventAggregationStream(), lowPriority(), master, propertyHandler));
        list.add(new PhoneBatteryStream(
                defs.getBatteryStream(), lowPriority(), master, propertyHandler));
        list.add(new PhoneAccelerationStream(
                defs.getAccelerationStream(), normalPriority(), master, propertyHandler));
    }
}
