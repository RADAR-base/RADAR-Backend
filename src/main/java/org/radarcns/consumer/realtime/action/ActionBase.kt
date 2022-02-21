package org.radarcns.consumer.realtime.action

import org.radarcns.config.realtime.ActionConfig

abstract class ActionBase(
        val config: ActionConfig,
        override val projects: List<String>? = config.projects,
        override val subjects: List<String>? = config.subjects,
) : Action