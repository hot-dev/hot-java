package dev.hot.sdk.internal;

import dev.hot.sdk.HotApiException;
import dev.hot.sdk.HotTransportException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandler;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Internal HTTP transport shared by the sync and async clients. Not part of the public API. */
public final class Transport {

  public static final String DEFAULT_BASE_URL = "https://api.hot.dev";

  // Implementation-Version comes from the jar manifest; "dev" outside a jar.
  public static final String USER_AGENT =
      "hot-sdk-java/" + versionOrDefault(Transport.class.getPackage().getImplementationVersion());

  // JSON requests retry on 429 when the server supplies retry_after.
  private static final int MAX_RETRIES = 2;
  private static final int MAX_RETRY_AFTER_SECONDS = 30;

  private static String versionOrDefault(String version) {
    return version != null ? version : "dev";
  }

  private final String token;
  private final String baseUrl;
  private final String apiBaseUrl;
  private final HttpClient httpClient;
  private final Duration timeout;

  public Transport(String token, String baseUrl, Duration timeout, HttpClient httpClient) {
    if (token == null || token.isEmpty()) {
      throw new IllegalArgumentException("token is required");
    }
    String trimmed = baseUrl;
    while (trimmed.endsWith("/")) {
      trimmed = trimmed.substring(0, trimmed.length() - 1);
    }
    this.token = token;
    this.baseUrl = trimmed;
    this.apiBaseUrl = trimmed + "/v1";
    this.httpClient = httpClient != null ? httpClient : HttpClient.newHttpClient();
    this.timeout = timeout;
  }

  public String baseUrl() {
    return baseUrl;
  }

  /**
   * Calls a JSON endpoint and returns the response envelope. {@code timeout} is applied here and
   * intentionally not to streaming or raw requests, which it would sever.
   */
  public Map<String, Object> requestJson(
      String method, String path, Object body, Map<String, String> query) {
    BodyPublisher publisher =
        body == null ? BodyPublishers.noBody() : BodyPublishers.ofString(Json.write(body));
    HttpRequest.Builder builder =
        newRequest(method, path, query, publisher).header("Accept", "application/json");
    if (body != null) {
      builder.header("Content-Type", "application/json");
    }
    if (timeout != null) {
      builder.timeout(timeout);
    }
    HttpRequest request = builder.build();

    for (int attempt = 0; ; attempt++) {
      HttpResponse<String> response = send(request, BodyHandlers.ofString());
      if (response.statusCode() < 400) {
        return Json.envelope(response.body());
      }
      HotApiException error =
          HotApiException.parse(response.statusCode(), response.body(), response.headers());
      if (response.statusCode() == 429
          && attempt < MAX_RETRIES
          && error.retryAfter() != null
          && error.retryAfter() > 0) {
        sleepSeconds(Math.min(error.retryAfter(), MAX_RETRY_AFTER_SECONDS));
        continue;
      }
      throw error;
    }
  }

  private static void sleepSeconds(long seconds) {
    try {
      Thread.sleep(seconds * 1000L);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new HotTransportException(interrupted);
    }
  }

  /**
   * Performs a request and returns the raw response. The caller must close the body stream.
   * Non-2xx responses become {@link HotApiException}.
   */
  public HttpResponse<InputStream> requestRaw(
      String method,
      String path,
      Map<String, String> query,
      Map<String, String> headers,
      BodyPublisher body) {
    HttpRequest.Builder builder = newRequest(method, path, query, body);
    if (headers != null) {
      headers.forEach(builder::header);
    }

    HttpResponse<InputStream> response = send(builder.build(), BodyHandlers.ofInputStream());
    if (response.statusCode() >= 400) {
      String text;
      try (InputStream stream = response.body()) {
        text = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
      } catch (IOException error) {
        text = "";
      }
      throw HotApiException.parse(response.statusCode(), text, response.headers());
    }
    return response;
  }

  /** Opens an SSE request and returns an event iterator. */
  public SseEvents stream(String method, String path, Object body, Map<String, String> query) {
    BodyPublisher publisher =
        body == null ? BodyPublishers.noBody() : BodyPublishers.ofString(Json.write(body));
    HttpRequest.Builder builder =
        newRequest(method, path, query, publisher).header("Accept", "text/event-stream");
    if (body != null) {
      builder.header("Content-Type", "application/json");
    }

    HttpResponse<Stream<String>> response = send(builder.build(), BodyHandlers.ofLines());
    if (response.statusCode() >= 400) {
      String text;
      try (Stream<String> lines = response.body()) {
        text = lines.collect(Collectors.joining("\n"));
      }
      throw HotApiException.parse(response.statusCode(), text, response.headers());
    }
    return new SseEvents(response);
  }

  private HttpRequest.Builder newRequest(
      String method, String path, Map<String, String> query, BodyPublisher body) {
    URI uri = URI.create(apiBaseUrl + path + queryString(query));
    return HttpRequest.newBuilder(uri)
        .method(method, body)
        .header("Authorization", "Bearer " + token)
        .header("User-Agent", USER_AGENT);
  }

  private <T> HttpResponse<T> send(HttpRequest request, BodyHandler<T> handler) {
    try {
      return httpClient.send(request, handler);
    } catch (IOException error) {
      throw new HotTransportException(error);
    } catch (InterruptedException error) {
      Thread.currentThread().interrupt();
      throw new HotTransportException(error);
    }
  }

  private static String queryString(Map<String, String> query) {
    if (query == null || query.isEmpty()) {
      return "";
    }
    StringJoiner joiner = new StringJoiner("&", "?", "");
    boolean any = false;
    for (Map.Entry<String, String> entry : query.entrySet()) {
      if (entry.getValue() == null) {
        continue;
      }
      joiner.add(enc(entry.getKey()) + "=" + enc(entry.getValue()));
      any = true;
    }
    return any ? joiner.toString() : "";
  }

  /** Percent-encodes a single path segment (or query component), including any "/". */
  public static String enc(String segment) {
    StringBuilder encoded = new StringBuilder();
    for (byte raw : segment.getBytes(StandardCharsets.UTF_8)) {
      int b = raw & 0xff;
      boolean unreserved =
          (b >= 'A' && b <= 'Z')
              || (b >= 'a' && b <= 'z')
              || (b >= '0' && b <= '9')
              || b == '-'
              || b == '_'
              || b == '.'
              || b == '~';
      if (unreserved) {
        encoded.append((char) b);
      } else {
        encoded.append('%').append(String.format("%02X", b));
      }
    }
    return encoded.toString();
  }

  /** Percent-encodes each segment of a slash-separated path, keeping the slashes. */
  public static String encPath(String path) {
    StringJoiner joiner = new StringJoiner("/");
    for (String part : path.split("/", -1)) {
      joiner.add(enc(part));
    }
    return joiner.toString();
  }
}
