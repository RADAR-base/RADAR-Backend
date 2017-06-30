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

package org.radarcns.phone.topic;

import org.radarcns.stream.GeneralStreamGroup;
import org.radarcns.stream.StreamDefinition;

public final class PhoneStreams extends GeneralStreamGroup {
    private static final PhoneStreams INSTANCE = new PhoneStreams();

    private final StreamDefinition usageEventStream;

    public static PhoneStreams getInstance() {
        return INSTANCE;
    }

    private PhoneStreams() {
        usageEventStream = createSensorStream(
                "android_phone_usage_event");
    }

    public StreamDefinition getUsageStream() {
        return usageEventStream;
    }
}
