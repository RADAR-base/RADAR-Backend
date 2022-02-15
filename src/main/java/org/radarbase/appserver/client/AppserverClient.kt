package org.radarbase.appserver.client

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import okhttp3.*
import okhttp3.Headers.Companion.headersOf
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.RequestBody.Companion.toRequestBody
import org.radarbase.exception.TokenException
import org.radarbase.oauth.OAuth2Client
import org.slf4j.LoggerFactory
import java.io.IOException

/**
 * Client to provide interface to the AppServer REST API. It uses the OAuth client library from
 * management portal to authenticate requests.
 */
class AppserverClient(config: AppserverClientConfig) {
    private val baseUrl: HttpUrl
    private val httpClient: OkHttpClient = config.httpClient ?: OkHttpClient()
    private val objectMapper: ObjectMapper = config.mapper ?: ObjectMapper()
    private val oauthClient: OAuth2Client?

    constructor(build: AppserverClientConfig.() -> Unit): this(AppserverClientConfig().apply(build))

    init {
        baseUrl = requireNotNull(config.appserverUrl) { "Appserver client needs base URL" }

        oauthClient = if (config.tokenUrl == null) {
            logger.warn("MP Token url was not provided. Will use unauthenticated requests to appserver.")
            null
        } else {
            val clientId = requireNotNull(config.clientId) { "Client ID for appserver client is not set." }
            val clientSecret = requireNotNull(config.clientSecret) { "Client secret for appserver client is not set." }
            OAuth2Client.Builder()
                .credentials(clientId, clientSecret)
                .endpoint(config.tokenUrl)
                .scopes("SUBJECT.READ SUBJECT.UPDATE PROJECT.READ")
                .httpClient(httpClient)
                .build()
        }
    }

    @Throws(TokenException::class)
    private fun headers(): Headers {
        return if (oauthClient != null) {
            headersOf("Authorization", "Bearer " + oauthClient.validToken.accessToken)
        } else {
            headersOf()
        }
    }

    @Throws(IOException::class)
    fun createMessage(
        projectId: String,
        userId: String,
        type: MessagingType,
        contents: AppServerMessageContents,
    ): Map<String, Any> {
        val stringContents = objectMapper.writeValueAsString(contents)
        return createMessage(projectId, userId, type, stringContents)
    }

    @Throws(IOException::class)
    fun createMessage(
        projectId: String,
        userId: String,
        type: MessagingType,
        contents: String,
    ): Map<String, Any> {
        val request: Request = try {
            Request.Builder()
                .post(contents.toRequestBody(APPLICATION_JSON))
                .url(
                    baseUrl.newBuilder()
                        .addEncodedPathSegment("projects")
                        .addPathSegment(projectId)
                        .addEncodedPathSegment("users")
                        .addPathSegment(userId)
                        .addEncodedPathSegment("messaging")
                        .addEncodedPathSegment(type.urlPart)
                        .build()
                )
                .headers(headers())
                .build()
        } catch (e: TokenException) {
            throw IOException(e)
        }
        return httpClient.newCall(request).execute().use { handleResponse(it) }
    }

    @Throws(IOException::class)
    fun getUserDetails(projectId: String, userId: String): Map<String, Any> {
        val request: Request = try {
            Request.Builder()
                .get()
                .url(
                    baseUrl.newBuilder()
                        .addEncodedPathSegment("projects")
                        .addPathSegment(projectId)
                        .addEncodedPathSegment("users")
                        .addPathSegment(userId)
                        .build()
                )
                .headers(headers())
                .build()
        } catch (e: TokenException) {
            throw IOException(e)
        }
        return httpClient.newCall(request).execute().use { handleResponse(it) }
    }

    @Throws(IOException::class)
    private fun handleResponse(response: Response): Map<String, Any> {
        val body = response.body
            ?: throw IOException("Response body from appserver was null.")

        return when (response.code) {
            404 -> throw IOException("The Entity or URL was not found in the appserver: $body")
            200, 201 -> objectMapper.readValue(body.string(), object : TypeReference<Map<String, Any>>() {})
            else -> throw IOException("There was an error requesting the appserver. $response")
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(AppserverClient::class.java)

        private val APPLICATION_JSON = "application/json".toMediaType()
    }
}
