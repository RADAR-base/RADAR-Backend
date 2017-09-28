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

import org.radarcns.stream.GeneralStreamGroup;
import org.radarcns.stream.StreamDefinition;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

public final class PhoneStreams extends GeneralStreamGroup {
    private static final PhoneStreams INSTANCE = new PhoneStreams();

    private final Collection<StreamDefinition> usageEventStream;
    private final Collection<StreamDefinition> usageEventAggregationStream;
    private final Collection<StreamDefinition> accelerationStream;
    private final Collection<StreamDefinition> batteryStream;

    public static PhoneStreams getInstance() {
        return INSTANCE;
    }

    private PhoneStreams() {
        usageEventStream = createSensorStream(
                "android_phone_usage_event");
        usageEventAggregationStream = createStream(
                "android_phone_usage_event_output",
                "android_phone_usage_event_aggregated",
                TimeUnit.DAYS.toMillis(1));
        accelerationStream = createWindowedSensorStream("android_phone_acceleration");
        batteryStream = createWindowedSensorStream("android_phone_battery_level");
    }

    public Collection<StreamDefinition> getUsageStream() {
        return usageEventStream;
    }

    public Collection<StreamDefinition> getUsageEventAggregationStream() {
        return usageEventAggregationStream;
    }

    public Collection<StreamDefinition> getAccelerationStream() {
        return accelerationStream;
    }

    public Collection<StreamDefinition> getBatteryStream() {
        return batteryStream;
    }
}
