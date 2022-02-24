package org.radarcns.consumer.realtime.condition

import org.radarcns.config.realtime.ConditionConfig

abstract class ConditionBase(
        config: ConditionConfig,
        override val projects: List<String>? = config.projects,
        override val subjects: List<String>? = config.subjects,
        override val subjectIdField: String? = config.projectIdField,
        override val projectIdField: String? = config.subjectIdField,
        override val timeField: String? = config.timeField,
        override val sourceIdField: String? = config.sourceIdField,
) : Condition