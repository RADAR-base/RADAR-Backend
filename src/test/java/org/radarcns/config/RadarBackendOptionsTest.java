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
