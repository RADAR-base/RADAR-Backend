package org.radarcns.consumer.realtime.action.appserver

import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit

/** Schedules the time based on current time with an optional added delay.  */
class SimpleTimeStrategy(
        delay: Long,
        unit: ChronoUnit?,
        private val delayDuration: Duration = Duration.of(delay, unit)
) : ScheduleTimeStrategy {
    override val scheduledTime: Instant
        get() = Instant.now().plus(delayDuration)
}