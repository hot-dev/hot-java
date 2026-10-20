package dev.hot.sdk;

import dev.hot.sdk.internal.Json;
import dev.hot.sdk.internal.Transport;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Map;

/**
 * Root management API client using an hm_ credential issued by a root admin. Use a separate
 * HotClient with a child environment key for runtime operations.
 */
public final class HotManagementClient {
  private final Transport transport;

  private HotManagementClient(Builder builder) {
    transport = new Transport(builder.token, builder.baseUrl, builder.timeout, builder.httpClient);
  }

  public static Builder builder(String token) {
    return new Builder(token);
  }

  /** Returns children and total for authorized targets, not a list envelope. */
  public Map<String, Object> listChildren(Map<String, String> query) {
    return data("GET", "/management/children", null, query);
  }

  /** Keep idempotency_key stable across retries. */
  public Map<String, Object> createChild(Map<String, Object> body) {
    return data("POST", "/management/children", body, null);
  }

  /** Set active, suspended, or archived. Success has no body. */
  public void setChildState(String childId, String state) {
    data(
        "PATCH",
        "/management/children/" + Transport.enc(childId) + "/state",
        Map.of("state", state),
        null);
  }

  public Map<String, Object> createEnvironment(String childId, Map<String, Object> body) {
    return data(
        "POST", "/management/children/" + Transport.enc(childId) + "/environments", body, null);
  }

  /** Issue an environment key within this credential's delegation ceiling. */
  public Map<String, Object> createApiKey(String childId, String envId, Map<String, Object> body) {
    return data(
        "POST",
        "/management/children/"
            + Transport.enc(childId)
            + "/environments/"
            + Transport.enc(envId)
            + "/api-keys",
        body,
        null);
  }

  /** Account aggregate; requires usage:read and explicit all_children authority. */
  public Map<String, Object> usage() {
    return data("GET", "/management/usage", null, null);
  }

  private Map<String, Object> data(
      String method, String path, Object body, Map<String, String> query) {
    return Json.dataObject(transport.requestJson(method, path, body, query));
  }

  public static final class Builder {
    private final String token;
    private String baseUrl = Transport.DEFAULT_BASE_URL;
    private Duration timeout;
    private HttpClient httpClient;

    private Builder(String token) {
      this.token = token;
    }

    public Builder baseUrl(String baseUrl) {
      this.baseUrl = baseUrl;
      return this;
    }

    public Builder timeout(Duration timeout) {
      this.timeout = timeout;
      return this;
    }

    public Builder httpClient(HttpClient httpClient) {
      this.httpClient = httpClient;
      return this;
    }

    public HotManagementClient build() {
      return new HotManagementClient(this);
    }
  }
}
