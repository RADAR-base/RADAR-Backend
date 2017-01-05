package org.radarcns.util;

import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.config.RadarPropertyHandlerImpl;

public final class RadarSingletonFactory {
    private static RadarUtilities utilities;

    private static RadarPropertyHandler propertyHandler;

    private static final Object SYNC_OBJECT = new Object();

    private RadarSingletonFactory() {
        // utility class
    }

    public static RadarUtilities getRadarUtilities() {
        synchronized (SYNC_OBJECT) {
            if (utilities == null) {
                utilities = new RadarUtils();
            }
            return utilities;
        }
    }

    public static RadarPropertyHandler getRadarPropertyHandler() {
        synchronized (SYNC_OBJECT) {
            if (propertyHandler == null) {
                propertyHandler = new RadarPropertyHandlerImpl();
            }
            return propertyHandler;
        }
    }

}
