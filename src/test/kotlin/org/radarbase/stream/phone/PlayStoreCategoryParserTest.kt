package org.radarbase.stream.phone

import com.fleeksoft.ksoup.Ksoup
import com.fleeksoft.ksoup.parseInputStream
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import java.util.zip.GZIPInputStream

class PlayStoreCategoryParserTest {
    private val BASE_URL = "https://play.google.com/store/apps/details?id=nl.thehyve.transmartclient"

    @Test
    fun getCategoryFromDocument() {
        val doc = javaClass.getResourceAsStream("/org/radarbase/stream/phone/transmart_app.html.gz")?.use { stream ->
            GZIPInputStream(stream).use { gzipStream ->
                Ksoup.parseInputStream(input = gzipStream, baseUri = BASE_URL, charsetName = "UTF-8")
            }
        } ?: throw IllegalStateException("Resource not found")

        val category = PlayStoreLookup.getCategoryFromDocument(doc, "nl.thehyve.transmartclient")
        assertEquals("MEDICAL", category.categoryName)
    }

    @Test
    fun getCategoryFromDocumentNoCategory() {
        val doc = javaClass.getResourceAsStream("/org/radarbase/stream/phone/transmart_app_no_category.html.gz")?.use { stream ->
            GZIPInputStream(stream).use { gzipStream -> Ksoup.parseInputStream(input = gzipStream, baseUri = BASE_URL, charsetName = "UTF-8")
            }
        } ?: throw IllegalStateException("Resource not found")

        val category = PlayStoreLookup.getCategoryFromDocument(doc, "nl.thehyve.transmartclient")
        assertNull(category.categoryName)
    }

    @Test
    fun getCategoryFromDocumentBroken() {
        val doc = javaClass.getResourceAsStream("/org/radarbase/stream/phone/transmart_app_broken.html.gz")?.use { stream ->
            GZIPInputStream(stream).use { gzipStream ->
                Ksoup.parseInputStream(input = gzipStream, baseUri = BASE_URL, charsetName = "UTF-8")
            }
        } ?: throw IllegalStateException("Resource not found")

        val category = PlayStoreLookup.getCategoryFromDocument(doc, "nl.thehyve.transmartclient")
        assertNull(category.categoryName)
    }
}
