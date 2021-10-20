package org.radarcns.consumer.realtime.action.appserver;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Map;
import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.radarbase.exception.TokenException;
import org.radarbase.oauth.OAuth2Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppserverClient {

  private static final Logger logger = LoggerFactory.getLogger(AppserverClient.class);

  private final String baseUrl;
  private final OkHttpClient httpClient;
  private final ObjectMapper objectMapper;
  private final OAuth2Client oauthClient;

  public AppserverClient(String baseUrl, String tokenUrl, String clientId, String clientSecret)
      throws MalformedURLException {
    this.baseUrl = baseUrl;
    httpClient = new OkHttpClient.Builder().build();
    objectMapper = new ObjectMapper();

    if (tokenUrl == null || tokenUrl.isEmpty()) {
      logger.warn("MP Token url was not provided. Will use unauthenticated requests to appserver.");
      this.oauthClient = null;
    } else {
      if (clientId.isEmpty()) {
        throw new IllegalArgumentException("Client ID for appserver client is not set.");
      }
      this.oauthClient =
          new OAuth2Client.Builder()
              .credentials(clientId, clientSecret)
              .endpoint(new URL(tokenUrl))
              .scopes("SUBJECT.READ SUBJECT.UPDATE PROJECT.READ")
              .httpClient(httpClient)
              .build();
    }
  }

  protected Headers headers() throws TokenException {
    if (oauthClient != null) {
      return Headers.of("Authorization", "Bearer " + oauthClient.getValidToken().getAccessToken());
    } else {
      return Headers.of();
    }
  }

  public Map<String, Object> createMessage(
      String projectId, String userId, String type, String body) throws IOException {
    URI uri =
        URI.create(baseUrl)
            .resolve("/projects/" + projectId + "/users/" + userId + "/messaging/" + type);

    RequestBody requestBody = RequestBody.create(body, MediaType.parse("application/json"));

    Request request;
    try {
      request = new Request.Builder().post(requestBody).url(uri.toURL()).headers(headers()).build();
    } catch (TokenException e) {
      throw new IOException(e);
    }
    try (Response response = httpClient.newCall(request).execute()) {
      return handleResponse(response);
    }
  }

  public Map<String, Object> getUserDetails(String projectId, String userId) throws IOException {
    URI uri = URI.create(baseUrl).resolve("/projects/" + projectId + "/users/" + userId);

    Request request;
    try {
      request = new Request.Builder().get().url(uri.toURL()).headers(headers()).build();
    } catch (TokenException e) {
      throw new IOException(e);
    }

    try (Response response = httpClient.newCall(request).execute()) {
      return handleResponse(response);
    }
  }

  private Map<String, Object> handleResponse(Response response) throws IOException {

    switch (response.code()) {
      case 404:
        throw new IOException("The Entity or URL was not found in the appserver." + response);
      case 200:
      case 201:
        if (response.body() == null) {
          throw new IOException(
              "The response was success from appserver but body was null. " + response);
        }
        return objectMapper.readValue(response.body().string(), new TypeReference<>() {});
      default:
        throw new IOException("There was an error requesting the appserver. " + response);
    }
  }
}
