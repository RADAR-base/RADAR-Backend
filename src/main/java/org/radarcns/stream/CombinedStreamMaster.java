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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class CombinedStreamMaster extends StreamMaster {

    private final List<StreamMaster> streamMasters;
    private final StreamGroup streamGroup;

    public CombinedStreamMaster(Collection<StreamMaster> streamMasters) throws IOException {
        super(true, "Combined");
        if (streamMasters == null || streamMasters.isEmpty()) {
            throw new IllegalArgumentException("Stream workers collection may not be empty");
        }
        this.streamMasters = new ArrayList<>(streamMasters);
        this.streamGroup = new StreamGroup() {
            @Override
            public List<String> getTopicNames() {
                List<String> topics = new ArrayList<>();
                for (StreamMaster master : streamMasters) {
                    topics.addAll(master.getStreamGroup().getTopicNames());
                }
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
        };
    }

    @Override
    public List<StreamWorker<?,?>> createWorkers(int low, int normal, int high) {
        // Create AggregatorWorkers from all streamMasters added
        List<StreamWorker<?,?>> workers = new ArrayList<>();
        this.streamMasters.forEach(w -> workers.addAll(w.createWorkers(low, normal, high)));
        return workers;
    }

    @Override
    protected StreamGroup getStreamGroup() {
        return this.streamGroup;
    }
}
