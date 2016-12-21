package org.radarcns.util;

/**
 * Created by nivethika on 20-12-16.
 */
public class RadarSingletonFactory {

    private static RadarUtilities utilities;

    private RadarSingletonFactory() {
    }

    public static RadarUtilities getRadarUtilities() {
        if (utilities == null) {
            utilities = new RadarUtils();
        }
        return utilities;
    }

}
