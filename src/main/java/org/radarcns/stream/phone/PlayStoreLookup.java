/*
 * Copyright 2017 King's College London and The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarcns.stream.phone;

import java.io.IOException;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Google Play Store lookup backed by a cache.
 *
 * This implementation is thread-safe
 */
public final class PlayStoreLookup {
    private static final Logger log = LoggerFactory.getLogger(PlayStoreLookup.class);
    private static final String URL_PLAY_STORE_APP_DETAILS = "https://play.google.com/store/apps/details?id=";
    private static final String CATEGORY_ANCHOR_SELECTOR = "a.document-subtitle.category";

    private final long cacheTimeout;
    private final Cache<String, AppCategory> categoryCache;

    public PlayStoreLookup(long cacheTimeoutSeconds, int maxCacheSize) {
        this.cacheTimeout = cacheTimeoutSeconds * 1000L;
        this.categoryCache = new SynchronizedCache<>(new LRUCache<>(maxCacheSize));
    }

    /**
     * Looks up a category from the play store, if possible from cache.
     *
     * The app category may have a null category name:
     * - if the page could not be retrieved (not public)
     * - category element on play store is not available
     * - no connection can be made with the play store
     *
     * In the last event, the category name is not cached, so it will be retried in a next call.
     *
     * @param packageName name of the package as registered in the play store
     * @return category as given by the play store
     */
    public AppCategory lookupCategory(String packageName) {
        AppCategory category = categoryCache.get(packageName);

        // If not yet in cache, fetch category for this package
        double cacheThreshold = (System.currentTimeMillis() - cacheTimeout) / 1000d;
        if (category == null || category.fetchTimeStamp < cacheThreshold) {
            try {
                category = fetchCategory(packageName);
                categoryCache.put(packageName, category);
            } catch (IOException ex) {
                // do not cache anything: we might have better luck next time
                log.warn("Could not find category of {}: {}", packageName, ex.toString());
                category = new AppCategory(null);
            }
        }

        return category;
    }

    /**
     * Fetches the app category by parsing apps Play Store page.
     *
     * The app category may have a null category name:
     * - if the page could not be retrieved (not public)
     * - category element on play store is not available
     *
     * @param packageName name of the package as registered in the play store
     * @return category as given by the play store
     * @throws IOException if no connection can be made with the Google App Store
     */
    public static AppCategory fetchCategory(String packageName) throws IOException {
        String url = URL_PLAY_STORE_APP_DETAILS + packageName;

        try {
            Document doc = Jsoup.connect(url).get();

            return getCategoryFromDocument(doc, packageName);
        } catch (HttpStatusException ex) {
            log.warn("Package {} page could not be found", packageName);
            return new AppCategory(null);
        }
    }

    /**
     * Retrieve an AppCategory from a parsed play store document.
     * @param doc parsed play store document
     * @param packageName package name of the document being parsed
     * @return the app category, with a null category name if the document did not contain a
     *         category.
     */
    static AppCategory getCategoryFromDocument(Document doc, String packageName) {
        Element categoryElement = doc.select(CATEGORY_ANCHOR_SELECTOR).first();

        if (categoryElement != null) {
            String href = categoryElement.attr("href");
            if (href != null) {
                String[] urlSplit = href.split("/");
                return new AppCategory(urlSplit[urlSplit.length - 1]);
            }
        }
        log.warn("Could not find category of {}: "
                + "element containing category could not be found", packageName);
        return new AppCategory(null);
    }

    /** Android app category. */
    public static class AppCategory {
        private final String categoryName;
        private final double fetchTimeStamp;

        private AppCategory(String categoryName) {
            this.categoryName = categoryName;
            this.fetchTimeStamp = System.currentTimeMillis() / 1000d;
        }

        /**
         * Get the app category name.
         *
         * @return category or null if the category is unknown.
         */
        public String getCategoryName() {
            return categoryName;
        }

        /**
         * Get the time that the app category was fetched from the Google Play Store.
         * @return time from unix epoch UTC, in seconds
         */
        public double getFetchTimeStamp() {
            return fetchTimeStamp;
        }
    }
}

