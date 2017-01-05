package org.radarcns.util;

public final class Serialization {
    private Serialization() {
        // utility class only
    }

    public static long bytesToLong(byte[] b) {
        long result = 0;
        for (int i = 0; i < 8; i++) {
            result <<= 8;
            result |= b[i] & 0xFF;
        }
        return result;
    }
}
