/*
 * Copyright 2017 Kings College London and The Hyve
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

package org.radarcns.empatica.topic;

import org.radarcns.topic.GeneralStreamGroup;
import org.radarcns.topic.StreamDefinition;

/**
 * Entire set of Empatica E4 InternalTopic
 */
public final class E4InternalStreams extends GeneralStreamGroup {
    private final StreamDefinition heartRateStream;

    private static E4InternalStreams instance = new E4InternalStreams();

    static E4InternalStreams getInstance() {
        return instance;
    }

    private E4InternalStreams() {
        heartRateStream = createStream(
                "android_empatica_e4_inter_beat_interval",
                "android_empatica_e4_heartrate");
    }

    public StreamDefinition getHeartRateStream() {
        return heartRateStream;
    }
}
