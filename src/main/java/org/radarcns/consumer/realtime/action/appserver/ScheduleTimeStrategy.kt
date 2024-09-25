package org.radarcns.consumer.realtime.action.appserver

import java.time.Instant

/**
 * The time calculation strategy for scheduling the notification via the [ ].
 *
 *
 * See [SimpleTimeStrategy], [TimeOfDayStrategy]
 */
interface ScheduleTimeStrategy {
    val scheduledTime: Instant
}