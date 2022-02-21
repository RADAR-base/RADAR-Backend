package org.radarcns.consumer.realtime.action.appserver

import java.time.*
import java.time.format.DateTimeFormatter

/**
 * Schedules the time based on the next Time Of Day (eg- 09:00:00 means 9 am on the day) as
 * configured in the configuration file. If the time of day specified has passed for the current
 * day, it will schedule at the time of day the next day.
 */
class TimeOfDayStrategy(
        private val timeOfDay: String,
        private val timezone: String = "GMT")
    : ScheduleTimeStrategy {

    // If time has already passed, schedule the next day
    override val scheduledTime: Instant
        get() {
            val now = Instant.now()
            val localDate = now.atZone(ZoneId.of(timezone)).toLocalDate()
            var ldt = localDate.atTime(LocalTime.parse(timeOfDay, DateTimeFormatter.ISO_LOCAL_TIME))

            // If time has already passed, schedule the next day
            if (ldt.isBefore(LocalDateTime.from(now))) {
                ldt = ldt.plusDays(1)
            }
            return ldt.toInstant(ZoneOffset.of(timezone))
        }

    init {
        require(timeOfDay.isNotEmpty()) { "The time of day is not provided. Cannot use this strategy." }
    }
}