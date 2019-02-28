/*
 * Copyright 2017 King's College London and The Hyve
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
public class RadarSingleton {
    private static final RadarSingleton INSTANCE = new RadarSingleton();

    private final RadarUtilities utilities;
    private final RadarPropertyHandler propertyHandler;

    @SuppressWarnings("WeakerAccess")
    protected RadarSingleton() {
        utilities = new RadarUtilitiesImpl();
        propertyHandler = new RadarPropertyHandlerImpl();
    }

    public static RadarSingleton getInstance() {
        return INSTANCE;
    }

    /**
     * Returns the singleton object of RadarUtilities
     *
     * @return a RadarUtilities object
     */
    public RadarUtilities getRadarUtilities() {
        return utilities;
    }

    /**
     * Returns the singleton object of RadarPropertyHandler
     *
     * @return a RadarPropertyHandler object
     */
    public RadarPropertyHandler getRadarPropertyHandler() {
        return propertyHandler;
    }
}
