package org.radarcns.util;

import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.config.RadarPropertyHandlerImpl;

/**
 * SingletonFactory of RadarBackend project. This factory composites all singleton objects that need
 * to be maintained in this project and provides a gateway to get singleton objects
 */
public class RadarSingletonFactory {

    private static RadarUtilities utilities;

    private static RadarPropertyHandler propertyHandler;

    private static final Object syncObject = new Object();

    private RadarSingletonFactory() {
    }

    /**
     * Returns the singleton object of RadarUtilities
     *
     * @return a RadarUtilities object
     */
    public static RadarUtilities getRadarUtilities() {
        synchronized (syncObject) {
            if (utilities == null) {
                utilities = new RadarUtilitiesImpl();
            }
            return utilities;
        }
    }

    /**
     * Returns the singleton object of RadarPropertyHandler
     *
     * @return a RadarPropertyHandler object
     */
    public static RadarPropertyHandler getRadarPropertyHandler() {
        synchronized (syncObject) {
            if (propertyHandler == null) {
                propertyHandler = new RadarPropertyHandlerImpl();
            }
            return propertyHandler;
        }
    }

}
