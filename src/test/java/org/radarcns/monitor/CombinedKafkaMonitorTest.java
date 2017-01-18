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

package org.radarcns.monitor;

import static org.junit.Assert.*;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;

public class CombinedKafkaMonitorTest {

    @Test(expected = IOException.class)
    public void testFlow() throws Exception {
        KafkaMonitor kafkaMonitor1 = mock(KafkaMonitor.class);
        KafkaMonitor kafkaMonitor2 = mock(KafkaMonitor.class);

        doThrow(new IOException("failed to run!")).when(kafkaMonitor2).start();

        CombinedKafkaMonitor km = new CombinedKafkaMonitor(Arrays.asList(kafkaMonitor1, kafkaMonitor2));

        try {
            km.start();
        } catch (IOException ex) {
            verify(kafkaMonitor1, times(1)).start();
            verify(kafkaMonitor2, times(1)).start();
            verify(kafkaMonitor1, times(1)).shutdown();
            verify(kafkaMonitor2, times(1)).shutdown();
            assertTrue(km.isShutdown());
            throw ex;
        }
    }

    @Test(expected = IOException.class)
    public void testFlow2() throws Exception {
        KafkaMonitor kafkaMonitor1 = mock(KafkaMonitor.class);
        KafkaMonitor kafkaMonitor2 = mock(KafkaMonitor.class);

        doThrow(new IOException("failed to run!")).when(kafkaMonitor1).start();

        CombinedKafkaMonitor km = new CombinedKafkaMonitor(Arrays.asList(kafkaMonitor1, kafkaMonitor2));

        try {
            km.start();
        } catch (IOException ex) {
            verify(kafkaMonitor1, times(1)).start();
            verify(kafkaMonitor2, times(1)).start();
            verify(kafkaMonitor1, times(1)).shutdown();
            verify(kafkaMonitor2, times(1)).shutdown();
            assertTrue(km.isShutdown());
            throw ex;
        }
    }


    @Test
    public void testPollTimeout() throws Exception {
        KafkaMonitor kafkaMonitor1 = mock(KafkaMonitor.class);
        KafkaMonitor kafkaMonitor2 = mock(KafkaMonitor.class);

        CombinedKafkaMonitor km = new CombinedKafkaMonitor(Arrays.asList(kafkaMonitor1, kafkaMonitor2));
        km.setPollTimeout(1000L);

        verify(kafkaMonitor1, times(1)).setPollTimeout(1000L);
        verify(kafkaMonitor2, times(1)).setPollTimeout(1000L);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmpty() {
        new CombinedKafkaMonitor(Collections.emptyList());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNull() {
        new CombinedKafkaMonitor(null);
    }
}