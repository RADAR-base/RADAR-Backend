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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** Combine multiple StreamMasters into a single object. */
public class CombinedStreamMaster extends StreamMaster {

    private final Collection<StreamMaster> streamMasters;
    private final StreamGroup streamGroup;

    /**
     * Create a stream master that will act as a master over given stream masters.
     * @param streamMasters stream masters to take care of
     */
    public CombinedStreamMaster(Collection<StreamMaster> streamMasters) {
        super(true, "Combined");
        if (streamMasters == null || streamMasters.isEmpty()) {
            throw new IllegalArgumentException("Stream workers collection may not be empty");
        }
        this.streamMasters = streamMasters;
        this.streamGroup = new CombinedStreamGroup();
    }

    @Override
    protected void createWorkers(List<StreamWorker<?,?>> list, int low, int normal, int high) {
        for (StreamMaster master : streamMasters) {
            master.createWorkers(list, low, normal, high);
        }
    }

    @Override
    protected StreamGroup getStreamGroup() {
        return this.streamGroup;
    }

    /** A stream group that combines the stream groups of the stream masters it is managing. */
    private class CombinedStreamGroup implements StreamGroup {
        @Override
        public List<String> getTopicNames() {
            List<String> topics = new ArrayList<>();
            for (StreamMaster master : streamMasters) {
                topics.addAll(master.getStreamGroup().getTopicNames());
            }
            topics.sort(String.CASE_INSENSITIVE_ORDER);
            return topics;
        }

        @Override
        public StreamDefinition getStreamDefinition(String inputTopic) {
            for (StreamMaster master : streamMasters) {
                if (master.getStreamGroup().getTopicNames().contains(inputTopic)) {
                    return master.getStreamGroup().getStreamDefinition(inputTopic);
                }
            }
            throw new IllegalArgumentException("Stream definition for input topic " + inputTopic
                    + " not found");
        }
    }
}
