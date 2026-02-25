package org.radarbase.stream.phone

import com.fleeksoft.ksoup.Ksoup
import com.fleeksoft.ksoup.network.parseGetRequestBlocking
import com.fleeksoft.ksoup.nodes.Document
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.concurrent.ConcurrentHashMap

/**
 * A Google Play Store lookup backed by a cache.
 *
 * This implementation is thread-safe.
 */
class PlayStoreLookup(private val cacheTimeoutSeconds: Long, maxCacheSize: Int) {
    private val cacheTimeout: Long = cacheTimeoutSeconds * 1000L
    private val categoryCache: MutableMap<String, AppCategory> = ConcurrentHashMap(maxCacheSize)

    fun lookupCategory(packageName: String): AppCategory {
        val category = categoryCache[packageName]
        val cacheThreshold = (System.currentTimeMillis() - cacheTimeout) / 1000.0
        
        return if (category == null || category.fetchTimeStamp < cacheThreshold) {
            try {
                fetchCategory(packageName).also {
                    categoryCache[packageName] = it
                }
            } catch (ex: IOException) {
                logger.warn("Could not find category of {}: {}", packageName, ex.toString())
                AppCategory(null)
            }
        } else {
            category
        }
    }

    /** Android app category.  */
    data class AppCategory(
        val categoryName: String?,
        val fetchTimeStamp: Double = System.currentTimeMillis() / 1000.0
    )

    companion object {
        private val logger = LoggerFactory.getLogger(PlayStoreLookup::class.java)
        private const val URL_PLAY_STORE_APP_DETAILS = "https://play.google.com/store/apps/details?id="
        private const val CATEGORY_ANCHOR_SELECTOR = "a[itemprop='genre']"

        @Throws(IOException::class)
        fun fetchCategory(packageName: String): AppCategory {
            val url = URL_PLAY_STORE_APP_DETAILS + packageName
            return try {
                val doc = Ksoup.parseGetRequestBlocking(url = url)
                getCategoryFromDocument(doc, packageName)
            } catch (ex: Exception) {
                logger.warn("Package {} page could not be found", packageName)
                AppCategory(null)
            }
        }

        internal fun getCategoryFromDocument(doc: Document, packageName: String): AppCategory {
            val categoryElement = doc.select(CATEGORY_ANCHOR_SELECTOR).first()
            if (categoryElement != null) {
                val href = categoryElement.attr("href")
                val urlSplit = href.split("/")
                return AppCategory(urlSplit.last())
            }
            logger.warn("Could not find category of {}: element containing category could not be found", packageName)
            return AppCategory(null)
        }
    }
}
