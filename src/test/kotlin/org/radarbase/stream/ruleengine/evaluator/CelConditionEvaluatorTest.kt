package org.radarbase.stream.ruleengine.evaluator

import org.junit.jupiter.api.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class CelConditionEvaluatorTest {

    @Test
    fun `expression referencing value fields evaluates correctly`() {
        val result = CelConditionEvaluator.isTrueFor(
            key = emptyMap<String, Any?>(),
            value = mapOf("heartRate" to 130L),
            expression = "value.heartRate > 120",
        )
        assertTrue(result)
    }

    @Test
    fun `expression referencing key fields compiles and evaluates correctly`() {
        val result = CelConditionEvaluator.isTrueFor(
            key = mapOf("projectId" to "project-1"),
            value = emptyMap<String, Any?>(),
            expression = "key.projectId == 'project-1'",
        )
        assertTrue(result)
    }

    @Test
    fun `expression referencing key fields that do not match evaluates false`() {
        val result = CelConditionEvaluator.isTrueFor(
            key = mapOf("projectId" to "project-1"),
            value = emptyMap<String, Any?>(),
            expression = "key.projectId == 'project-2'",
        )
        assertFalse(result)
    }

    @Test
    fun `compiled program is cached and reused for the same expression`() {
        val expression = "value.heartRate > ${System.nanoTime()}"
        CelConditionEvaluator.isTrueFor(emptyMap<String, Any?>(), mapOf("heartRate" to 0L), expression)
        val firstProgram = programFor(expression)

        CelConditionEvaluator.isTrueFor(emptyMap<String, Any?>(), mapOf("heartRate" to 1L), expression)
        val secondProgram = programFor(expression)

        assertTrue(firstProgram === secondProgram)
    }

    @Test
    fun `malformed expression throws instead of being swallowed`() {
        assertFailsWith<Exception> {
            CelConditionEvaluator.isTrueFor(emptyMap<String, Any?>(), emptyMap<String, Any?>(), "value.. invalid((")
        }
    }

    private fun programFor(expression: String): Any? {
        val field = CelConditionEvaluator.javaClass.getDeclaredField("programCache")
        field.isAccessible = true
        @Suppress("UNCHECKED_CAST")
        val cache = field.get(CelConditionEvaluator) as Map<String, Any?>
        return cache[expression]
    }
}
