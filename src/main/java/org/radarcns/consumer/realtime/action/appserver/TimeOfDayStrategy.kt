package org.radarcns.consumer.realtime.action.appserver

import java.time.Duration
import java.time.Instant
import java.time.LocalTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter

/**
 * Schedules the time based on the next Time Of Day (eg- 09:00:00 means 9 am on the day) as
 * configured in the configuration file. If the time of day specified has passed for the current
 * day, it will schedule at the time of day the next day.
 */
class TimeOfDayStrategy(
        private val timeOfDay: String,
        private val timezone: String = "GMT",
        private val jitter: Duration? = null,
        private val reference: Instant = Instant.now(),
) : ScheduleTimeStrategy {

    // If time has already passed, schedule the next day
    override val scheduledTime: Instant = getTimeOfDay()

    fun getTimeOfDay(): Instant {
        val now = reference
        val localDate = now.atZone(ZoneId.of(timezone)).toLocalDate()
        var ldt = localDate.atTime(LocalTime.parse(timeOfDay, DateTimeFormatter.ISO_LOCAL_TIME))
        if (jitter != null) {
            ldt = ldt.plus(jitter)
        }
        // If time has already passed, schedule the next day
        if (ldt.isBefore(now.atZone(ZoneId.of(timezone)).toLocalDateTime())) {
            ldt = ldt.plusDays(1)
        }

        return ldt.atZone(ZoneId.of(timezone)).toInstant()
    }

    init {
        require(timeOfDay.isNotEmpty()) { "The time of day is not provided. Cannot use this strategy." }
    }
}