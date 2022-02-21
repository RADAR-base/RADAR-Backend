package org.radarcns.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AppServerConfig {

    @JsonProperty("base_url")
    private String baseUrl;

    @JsonProperty("token_url")
    private String tokenUrl;

    @JsonProperty("client_id")
    private String clientId;

    @JsonProperty("client_secret")
    private String clientSecret;

    public String getBaseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public String getTokenUrl() {
        return tokenUrl;
    }

    public void setTokenUrl(String tokenUrl) {
        this.tokenUrl = tokenUrl;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getClientSecret() {
        return clientSecret;
    }

    public void setClientSecret(String clientSecret) {
        this.clientSecret = clientSecret;
    }

    public AppServerConfig withEnv() {
        String envClientId = System.getenv("APP_SERVER_CLIENT_ID");
        if (envClientId != null) {
            clientId = envClientId;
        }
        String envClientSecret = System.getenv("APP_SERVER_CLIENT_SECRET");
        if (envClientSecret != null) {
            clientSecret = envClientSecret;
        }
        return this;
    }
}
