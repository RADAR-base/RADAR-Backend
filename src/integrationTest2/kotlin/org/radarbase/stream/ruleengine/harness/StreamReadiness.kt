package org.radarbase.stream.ruleengine.harness

import org.apache.kafka.streams.KafkaStreams
import java.time.Duration
import java.time.Instant

/**
 * Waits for [KafkaStreams.state] to reach RUNNING (global-store restore complete) by polling a
 * real condition on a short interval, instead of a single blind sleep for a guessed duration.
 */
object StreamReadiness {
    private val POLL_INTERVAL: Duration = Duration.ofMillis(100)
    private val TERMINAL_STATES = setOf(KafkaStreams.State.ERROR, KafkaStreams.State.PENDING_SHUTDOWN, KafkaStreams.State.NOT_RUNNING)

    fun awaitRunning(streams: List<KafkaStreams>, timeout: Duration = Duration.ofSeconds(30)) {
        streams.forEach { awaitRunning(it, timeout) }
    }

    private fun awaitRunning(streams: KafkaStreams, timeout: Duration) {
        val deadline = Instant.now().plus(timeout)
        while (streams.state() != KafkaStreams.State.RUNNING) {
            check(streams.state() !in TERMINAL_STATES) {
                "KafkaStreams entered terminal state ${streams.state()} while waiting for RUNNING"
            }
            check(Instant.now().isBefore(deadline)) {
                "KafkaStreams did not reach RUNNING within $timeout (state=${streams.state()})"
            }
            Thread.sleep(POLL_INTERVAL.toMillis())
        }
    }
}
