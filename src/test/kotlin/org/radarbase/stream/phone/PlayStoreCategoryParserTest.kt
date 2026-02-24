package org.radarbase.stream.phone

import org.jsoup.Jsoup
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test
import org.radarbase.stream.phone.PlayStoreLookup
import java.util.zip.GZIPInputStream

class PlayStoreCategoryParserTest {
    private val BASE_URL = "https://play.google.com/store/apps/details?id=nl.thehyve.transmartclient"

    @Test
    fun getCategoryFromDocument() {
        val doc = javaClass.getResourceAsStream("/org/radarbase/stream/phone/transmart_app.html.gz")?.use { stream ->
            GZIPInputStream(stream).use { gzipStream ->
                Jsoup.parse(gzipStream, "UTF-8", BASE_URL)
            }
        } ?: throw IllegalStateException("Resource not found")

        val category = PlayStoreLookup.getCategoryFromDocument(doc, "nl.thehyve.transmartclient")
        assertEquals("MEDICAL", category.categoryName)
    }

    @Test
    fun getCategoryFromDocumentNoCategory() {
        val doc = javaClass.getResourceAsStream("/org/radarbase/stream/phone/transmart_app_no_category.html.gz")?.use { stream ->
            GZIPInputStream(stream).use { gzipStream ->
                Jsoup.parse(gzipStream, "UTF-8", BASE_URL)
            }
        } ?: throw IllegalStateException("Resource not found")

        val category = PlayStoreLookup.getCategoryFromDocument(doc, "nl.thehyve.transmartclient")
        assertNull(category.categoryName)
    }

    @Test
    fun getCategoryFromDocumentBroken() {
        val doc = javaClass.getResourceAsStream("/org/radarbase/stream/phone/transmart_app_broken.html.gz")?.use { stream ->
            GZIPInputStream(stream).use { gzipStream ->
                Jsoup.parse(gzipStream, "UTF-8", BASE_URL)
            }
        } ?: throw IllegalStateException("Resource not found")

        val category = PlayStoreLookup.getCategoryFromDocument(doc, "nl.thehyve.transmartclient")
        assertNull(category.categoryName)
    }
}
