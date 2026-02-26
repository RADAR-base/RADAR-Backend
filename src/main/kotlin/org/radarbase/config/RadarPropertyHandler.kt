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

package org.radarbase.config

import org.radarbase.util.PersistentStateStore
import java.io.IOException

/**
 * Interface that handles YAML configuration file loading
 */
interface RadarPropertyHandler {

    enum class Priority(val param: String) {
        LOW("low"), NORMAL("normal"), HIGH("high")
    }

    val radarProperties: ConfigRadar

    @Throws(IOException::class)
    fun load(pathFile: String?)

    fun isLoaded(): Boolean

    val kafkaProperties: KafkaProperty

    /**
     * Create a [PersistentStateStore] if so configured in the radar properties. Notably, the
     * `persistence_path` must be set.
     * @return PersistentStateStore or null if none is configured.
     * @throws IOException if the persistence store cannot be reached.
     */
    @Throws(IOException::class)
    fun getPersistentStateStore(): PersistentStateStore?
}
