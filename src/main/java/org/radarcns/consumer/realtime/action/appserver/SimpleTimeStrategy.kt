package org.radarcns.consumer.realtime.action.appserver

import java.time.temporal.ChronoUnit
import java.time.Duration
import java.time.Instant

/** Schedules the time based on current time with an optional added delay.  */
class SimpleTimeStrategy(delay: Long, unit: ChronoUnit?) : ScheduleTimeStrategy {
    private val delay: Duration
    override val scheduledTime: Instant
        get() = Instant.now().plus(delay)

    init {
        this.delay = Duration.of(delay, unit)
    }
}