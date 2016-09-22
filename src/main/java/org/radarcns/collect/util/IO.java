package org.radarcns.collect.util;

import java.io.InputStream;
import java.util.Scanner;

public class IO {
    /**
     * Read all contents of a bounded input stream. Warning: this will not exit on an unbounded
     * stream. It uses the UTF-8 to interpret the stream.
     * @param in: bounded InputStream
     * @return the contents of the stream.
     */
    public static String readInputStream(InputStream in) {
        Scanner s = new Scanner(in, "UTF-8").useDelimiter("\\A");
        return s.hasNext() ? s.next() : "";
    }
}
