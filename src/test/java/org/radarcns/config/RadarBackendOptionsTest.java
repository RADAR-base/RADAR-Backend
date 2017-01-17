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

package org.radarcns.config;

import org.apache.commons.cli.ParseException;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class RadarBackendOptionsTest {
    @Test
    public void empty() throws ParseException {
        RadarBackendOptions opts = RadarBackendOptions.parse(new String[] {});
        assertNull(opts.getSubCommand());
        assertNull(opts.getSubCommandArgs());
        assertNull(opts.getPropertyPath());
    }

    @Test
    public void withConfig() throws ParseException {
        RadarBackendOptions opts = RadarBackendOptions.parse(new String[] {"-c", "cfg"});
        assertNull(opts.getSubCommand());
        assertNull(opts.getSubCommandArgs());
        assertEquals("cfg", opts.getPropertyPath());
    }

    @Test
    public void withSubcommand() throws ParseException {
        RadarBackendOptions opts = RadarBackendOptions.parse(new String[] {"-c", "cfg", "stream"});
        assertEquals("stream", opts.getSubCommand());
        assertArrayEquals(new String[0], opts.getSubCommandArgs());
        assertEquals("cfg", opts.getPropertyPath());
    }

    @Test
    public void withSubcommandArgs() throws ParseException {
        RadarBackendOptions opts = RadarBackendOptions.parse(
                new String[] {"monitor", "battery"});
        assertEquals("monitor", opts.getSubCommand());
        assertArrayEquals(new String[] {"battery"}, opts.getSubCommandArgs());
    }
}
