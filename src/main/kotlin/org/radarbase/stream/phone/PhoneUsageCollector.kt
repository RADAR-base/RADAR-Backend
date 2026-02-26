package org.radarbase.stream.phone

import org.radarcns.passive.phone.PhoneUsageEvent
import org.radarcns.passive.phone.UsageEventType
import java.math.BigDecimal
import java.math.MathContext

/**
 * Created by piotrzakrzewski on 27/07/2017.
 * Converted to Kotlin.
 */
class PhoneUsageCollector {
    var totalForegroundTime: BigDecimal = BigDecimal.ZERO
        private set

    var lastForegroundEvent: Double = 0.0
    var timesTurnedOn: Int = 0
    var categoryName: String? = null
    var categoryNameFetchTime: Double? = null

    /**
     * Updates the collector state based on a new [PhoneUsageEvent].
     */
    fun update(event: PhoneUsageEvent): PhoneUsageCollector {
        // Update category info if present
        event.categoryName?.let {
            this.categoryName = it
            this.categoryNameFetchTime = event.categoryNameFetchTime
        }

        when (event.eventType) {
            UsageEventType.FOREGROUND -> {
                timesTurnedOn++
                lastForegroundEvent = event.time
            }
            UsageEventType.BACKGROUND -> {
                if (lastForegroundEvent != 0.0) {
                    val duration = BigDecimal.valueOf(event.time)
                        .subtract(BigDecimal.valueOf(lastForegroundEvent), MathContext.DECIMAL128)
                    
                    totalForegroundTime = totalForegroundTime.add(duration)
                    lastForegroundEvent = 0.0
                }
            }
            else -> {
                // Ignore other event types or repeated backgrounds
            }
        }

        return this
    }

    // Helper property to match the original Java double getter/setter logic
    var totalForegroundTimeDouble: Double
        get() = totalForegroundTime.toDouble()
        set(value) {
            totalForegroundTime = BigDecimal.valueOf(value)
        }
}