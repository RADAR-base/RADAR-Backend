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

package org.radarcns.stream.aggregator;

import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class CombinedKafkaWorker extends MasterAggregator {

    private final List<MasterAggregator> streamWorkers;

    public CombinedKafkaWorker(Collection<MasterAggregator> streamWorkers) throws IOException {
        super(true, "Combined");
        if (streamWorkers == null || streamWorkers.isEmpty()) {
            throw new IllegalArgumentException("Stream workers collection may not be empty");
        }
        this.streamWorkers = new ArrayList<>(streamWorkers);
    }

    @Override
    public List<AggregatorWorker<?,?>> createWorkers(int low, int normal, int high) {
        // Create AggregatorWorkers from all streamWorkers added
        List<AggregatorWorker<?,?>> workers = new ArrayList<>();
        this.streamWorkers.forEach(w -> workers.addAll(w.createWorkers(low, normal, high)));
        return workers;
    }

    @Override
    protected void announceTopics(@Nonnull Logger log) {
        this.streamWorkers.forEach(w -> w.announceTopics(log));
    }
}
