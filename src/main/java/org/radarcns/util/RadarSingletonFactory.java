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

package org.radarcns.util;

import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.config.RadarPropertyHandlerImpl;

/**
 * SingletonFactory of RadarBackend project. This factory composites all singleton objects that need
 * to be maintained in this project and provides a gateway to get singleton objects
 */
public final class RadarSingletonFactory {

    private static RadarUtilities utilities;

    private static RadarPropertyHandler propertyHandler;

    private static final Object SYNC_OBJECT = new Object();

    private RadarSingletonFactory() {
        // utility class
    }

    /**
     * Returns the singleton object of RadarUtilities
     *
     * @return a RadarUtilities object
     */
    public static RadarUtilities getRadarUtilities() {
        synchronized (SYNC_OBJECT) {
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
        synchronized (SYNC_OBJECT) {
            if (propertyHandler == null) {
                propertyHandler = new RadarPropertyHandlerImpl();
            }
            return propertyHandler;
        }
    }

}
