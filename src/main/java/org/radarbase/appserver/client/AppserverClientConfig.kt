package org.radarbase.appserver.client

import com.fasterxml.jackson.databind.ObjectMapper
import okhttp3.HttpUrl
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.OkHttpClient
import java.net.MalformedURLException
import java.net.URL

class AppserverClientConfig {
    var appserverUrl: HttpUrl? = null
    var tokenUrl: URL? = null
    var clientId: String? = null
    var clientSecret: String? = null
    var httpClient: OkHttpClient? = null
        set(value) {
            field = value?.newBuilder()?.build()
        }
    var mapper: ObjectMapper? = null

    fun tokenUrl(url: String?) {
        tokenUrl = if (url != null) {
            try {
                URL(url)
            } catch (ex: MalformedURLException) {
                throw IllegalArgumentException(ex)
            }
        } else {
            null
        }
    }

    fun appserverUrl(url: String?) {
        appserverUrl = url?.toHttpUrl()
    }

    override fun toString(): String = "AppServerClientConfig(" +
            "appserverUrl=$appserverUrl, " +
            "tokenUrl=$tokenUrl, " +
            "clientId=$clientId, " +
            "clientSecret=$clientSecret)"
}
