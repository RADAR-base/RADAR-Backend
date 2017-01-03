package org.radarcns.util;

import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.config.RadarPropertyHandlerImpl;

public class RadarSingletonFactory {

    private static RadarUtilities utilities;

    private static RadarPropertyHandler propertyHandler;

    private static final Object syncObject = new Object();

    private RadarSingletonFactory() {
    }

    public static RadarUtilities getRadarUtilities() {
        synchronized (syncObject) {
            if (utilities == null) {
                utilities = new RadarUtils();
            }
            return utilities;
        }
    }

    public static RadarPropertyHandler getRadarPropertyHandler() {
        synchronized (syncObject) {
            if (propertyHandler == null) {
                propertyHandler = new RadarPropertyHandlerImpl();
            }
            return propertyHandler;
        }
    }

}
