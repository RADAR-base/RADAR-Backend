package org.radarcns.util;

import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.config.RadarPropertyHandlerImpl;

/**
 * Created by nivethika on 20-12-16.
 */
public class RadarSingletonFactory {

    private static RadarUtilities utilities;

    private static RadarPropertyHandler propertyHandler;


    private RadarSingletonFactory() {
    }

    public static RadarUtilities getRadarUtilities() {
        if (utilities == null) {
            utilities = new RadarUtils();
        }
        return utilities;
    }

    public static RadarPropertyHandler getRadarPropertyHandler() {
        if (propertyHandler == null) {
            propertyHandler = new RadarPropertyHandlerImpl();
        }
        return propertyHandler;
    }

}
