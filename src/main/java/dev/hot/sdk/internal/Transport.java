package dev.hot.sdk.internal;

import dev.hot.sdk.HotApiException;
import dev.hot.sdk.HotTransportException;
import java.io.ByteArrayOutputStream;
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
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/** Internal HTTP transport shared by the sync and async clients. Not part of the public API. */
public final class Transport {

  public static final String DEFAULT_BASE_URL = "https://api.hot.dev";

  // Implementation-Version comes from the jar manifest; "dev" outside a jar.
  public static final String USER_AGENT =
      "hot-sdk-java/" + versionOrDefault(Transport.class.getPackage().getImplementationVersion());

  // JSON requests retry on 429 when the server supplies retry_after, POST retries on
  // 503 event_publish_in_progress, and idempotent JSON requests also retry on 502/503/504.
  private static final int MAX_RETRIES = 2;
  private static final int MAX_RETRY_AFTER_SECONDS = 30;
  private static final Set<String> IDEMPOTENT_METHODS =
      Set.of("GET", "PUT", "DELETE", "HEAD", "OPTIONS");

  /** Maximum bytes read from a non-2xx error response body (1 MiB). */
  static final int MAX_ERROR_BODY_BYTES = 1024 * 1024;

  /** Default bound for each JSON request when the builder sets no timeout. */
  public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(60);

  /** Connect timeout of the default HttpClient. */
  public static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(10);

  // Shared lazily across default HotClient instances so per-sub-org clients do not leak
  // HttpClient selector threads and connection pools.
  private static final class DefaultHttpClient {
    static final HttpClient INSTANCE =
        HttpClient.newBuilder()
            .connectTimeout(DEFAULT_CONNECT_TIMEOUT)
            .followRedirects(HttpClient.Redirect.NEVER)
            .build();
  }

  private static String versionOrDefault(String version) {
    return version != null ? version : "dev";
  }

  private final String token;
  private final String baseUrl;
  private final String apiBaseUrl;
  private final HttpClient httpClient;
  private final Duration timeout;
  private final boolean bigDecimals;

  public Transport(String token, String baseUrl, Duration timeout, HttpClient httpClient) {
    this(token, baseUrl, timeout, httpClient, false);
  }

  /**
   * {@code timeout} bounds each JSON request ({@link #DEFAULT_TIMEOUT} when null). With
   * {@code bigDecimals}, JSON floats decode as {@link java.math.BigDecimal}.
   */
  public Transport(
      String token,
      String baseUrl,
      Duration timeout,
      HttpClient httpClient,
      boolean bigDecimals) {
    if (token == null || token.isEmpty()) {
      throw new IllegalArgumentException("token is required");
    }
    if (timeout != null && (timeout.isZero() || timeout.isNegative())) {
      throw new IllegalArgumentException("timeout must be positive");
    }
    if (httpClient != null && httpClient.followRedirects() != HttpClient.Redirect.NEVER) {
      throw new IllegalArgumentException(
          "httpClient must use Redirect.NEVER to avoid forwarding Authorization headers");
    }
    String trimmed =
        baseUrl == null || baseUrl.isBlank() ? DEFAULT_BASE_URL : baseUrl.trim();
    while (trimmed.endsWith("/")) {
      trimmed = trimmed.substring(0, trimmed.length() - 1);
    }
    if (trimmed.endsWith("/v1")) {
      trimmed = trimmed.substring(0, trimmed.length() - 3);
      while (trimmed.endsWith("/")) {
        trimmed = trimmed.substring(0, trimmed.length() - 1);
      }
    }
    this.token = token;
    this.baseUrl = trimmed;
    this.apiBaseUrl = trimmed + "/v1";
    this.httpClient = httpClient != null ? httpClient : DefaultHttpClient.INSTANCE;
    this.timeout = timeout != null ? timeout : DEFAULT_TIMEOUT;
    this.bigDecimals = bigDecimals;
  }

  public String baseUrl() {
    return baseUrl;
  }

  /** Whether JSON floats decode as BigDecimal for this client. */
  public boolean bigDecimals() {
    return bigDecimals;
  }

  /** Package-private accessor for tests verifying default HttpClient sharing. */
  HttpClient httpClient() {
    return httpClient;
  }

  /**
   * Calls the public {@code GET /status} health endpoint on the server root without sending an
   * {@code Authorization} header, returning the unwrapped JSON map.
   */
  public Map<String, Object> status() {
    HttpRequest.Builder builder =
        HttpRequest.newBuilder(URI.create(baseUrl + "/status"))
            .GET()
            .header("User-Agent", USER_AGENT)
            .header("Accept", "application/json");
    return sendJsonWithRetries(builder, "GET", "/status");
  }

  /**
   * Calls a JSON endpoint and returns the response envelope. {@code timeout} is applied here and
   * intentionally not to streaming or raw requests, which it would sever.
   */
  public Map<String, Object> requestJson(
      String method, String path, Object body, Map<String, String> query) {
    String upperMethod = normalizeMethod(method);
    String normalizedPath = normalizePath(path);
    BodyPublisher publisher =
        body == null ? BodyPublishers.noBody() : BodyPublishers.ofString(Json.write(body));
    HttpRequest.Builder builder =
        newRequest(upperMethod, normalizedPath, query, publisher)
            .header("Accept", "application/json");
    if (body != null) {
      builder.header("Content-Type", "application/json");
    }
    return sendJsonWithRetries(builder, upperMethod, normalizedPath);
  }

  private Map<String, Object> sendJsonWithRetries(
      HttpRequest.Builder builder, String upperMethod, String path) {
    boolean idempotent = IDEMPOTENT_METHODS.contains(upperMethod);
    long deadline = System.nanoTime() + timeout.toNanos();

    for (int attempt = 0; ; attempt++) {
      Duration attemptTimeout;
      if (attempt == 0) {
        attemptTimeout = timeout;
      } else {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
          throw new HotTransportException(
              "request timeout exceeded before retry attempt " + attempt, null);
        }
        attemptTimeout = Duration.ofNanos(Math.max(1_000_000L, remaining));
      }

      HttpRequest request = builder.copy().timeout(attemptTimeout).build();
      HttpResponse<InputStream> response = send(request, BodyHandlers.ofInputStream());
      int status = response.statusCode();
      if (status >= 200 && status < 300) {
        String bodyText = readUtf8(response.body());
        return Json.envelope(bodyText, bigDecimals);
      }

      String errorText = readBoundedUtf8(response.body(), MAX_ERROR_BODY_BYTES);
      HotApiException error =
          HotApiException.parse(status, errorText, response.headers(), path);
      Integer retryAfter = error.retryAfter();

      long delayMillis = -1L;
      if (attempt < MAX_RETRIES && status == 429 && retryAfter != null && retryAfter > 0) {
        delayMillis = retry429DelayMillis(retryAfter);
      } else if (attempt < MAX_RETRIES
          && "POST".equals(upperMethod)
          && status == 503
          && "event_publish_in_progress".equals(error.code())) {
        delayMillis =
            retryAfter != null && retryAfter > 0
                ? Math.min(retryAfter, MAX_RETRY_AFTER_SECONDS) * 1000L
                : Backoff.delayMillis(attempt + 1);
      } else if (attempt < MAX_RETRIES
          && idempotent
          && (status == 502 || status == 503 || status == 504)) {
        long backoff = Backoff.delayMillis(attempt + 1);
        if (retryAfter != null && retryAfter > 0) {
          backoff = Math.max(backoff, Math.min(retryAfter, MAX_RETRY_AFTER_SECONDS) * 1000L);
        }
        delayMillis = backoff;
      }

      if (delayMillis >= 0) {
        long remainingNanos = deadline - System.nanoTime();
        if (remainingNanos > TimeUnit.MILLISECONDS.toNanos(delayMillis)) {
          sleepMillis(delayMillis);
          if (deadline - System.nanoTime() > 0) {
            continue;
          }
        }
      }
      throw error;
    }
  }

  /** 429 retry sleep: honors {@code Retry-After} with small positive jitter, capped at 30 s. */
  static long retry429DelayMillis(int retryAfterSeconds) {
    long base = Math.min(Math.max(retryAfterSeconds, 0), MAX_RETRY_AFTER_SECONDS) * 1000L;
    long maxJitter = Math.min(base / 4, 1_000L);
    long jitter = maxJitter > 0 ? ThreadLocalRandom.current().nextLong(maxJitter + 1) : 0L;
    return Math.min(base + jitter, MAX_RETRY_AFTER_SECONDS * 1000L);
  }

  private static void sleepMillis(long millis) {
    try {
      Thread.sleep(millis);
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
    String upperMethod = normalizeMethod(method);
    String normalizedPath = normalizePath(path);
    BodyPublisher effectiveBody = body != null ? body : BodyPublishers.noBody();
    HttpRequest.Builder builder = newRequest(upperMethod, normalizedPath, query, effectiveBody);
    if (headers != null) {
      for (Map.Entry<String, String> entry : headers.entrySet()) {
        if (entry.getKey() != null && entry.getValue() != null) {
          builder.setHeader(entry.getKey(), entry.getValue());
        }
      }
    }

    HttpResponse<InputStream> response = send(builder.build(), BodyHandlers.ofInputStream());
    int status = response.statusCode();
    if (status < 200 || status >= 300) {
      String text = readBoundedUtf8(response.body(), MAX_ERROR_BODY_BYTES);
      throw HotApiException.parse(status, text, response.headers(), normalizedPath);
    }
    return response;
  }

  /** Opens an SSE request and returns an event iterator. */
  public SseEvents stream(String method, String path, Object body, Map<String, String> query) {
    return stream(method, path, body, query, null, null);
  }

  /**
   * Opens an SSE request. {@code headerTimeout} bounds the wait for response headers (it does not
   * limit the body); {@code idleTimeout} fails the stream when no line, keepalives included,
   * arrives for that long. Either may be null.
   */
  public SseEvents stream(
      String method,
      String path,
      Object body,
      Map<String, String> query,
      Duration headerTimeout,
      Duration idleTimeout) {
    String upperMethod = normalizeMethod(method);
    String normalizedPath = normalizePath(path);
    BodyPublisher publisher =
        body == null ? BodyPublishers.noBody() : BodyPublishers.ofString(Json.write(body));
    HttpRequest.Builder builder =
        newRequest(upperMethod, normalizedPath, query, publisher)
            .header("Accept", "text/event-stream");
    if (body != null) {
      builder.header("Content-Type", "application/json");
    }
    if (headerTimeout != null) {
      builder.timeout(headerTimeout);
    }

    HttpResponse<InputStream> response = send(builder.build(), BodyHandlers.ofInputStream());
    int status = response.statusCode();
    if (status < 200 || status >= 300) {
      String text = readBoundedUtf8(response.body(), MAX_ERROR_BODY_BYTES);
      throw HotApiException.parse(status, text, response.headers(), normalizedPath);
    }
    return new SseEvents(response, bigDecimals, idleTimeout);
  }

  private HttpRequest.Builder newRequest(
      String upperMethod, String normalizedPath, Map<String, String> query, BodyPublisher body) {
    URI uri = URI.create(apiBaseUrl + normalizedPath + queryString(query));
    return HttpRequest.newBuilder(uri)
        .method(upperMethod, body)
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

  private static String readUtf8(InputStream stream) {
    try (InputStream in = stream) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException error) {
      throw new HotTransportException(error);
    }
  }

  static String readBoundedUtf8(InputStream stream, int maxBytes) {
    try (InputStream in = stream) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] buffer = new byte[8192];
      int remaining = maxBytes;
      while (remaining > 0) {
        int read = in.read(buffer, 0, Math.min(buffer.length, remaining));
        if (read < 0) {
          break;
        }
        out.write(buffer, 0, read);
        remaining -= read;
      }
      return out.toString(StandardCharsets.UTF_8);
    } catch (IOException error) {
      return "";
    }
  }

  private static String normalizeMethod(String method) {
    if (method == null || method.isBlank()) {
      throw new IllegalArgumentException("HTTP method is required");
    }
    return method.trim().toUpperCase(Locale.ROOT);
  }

  private static String normalizePath(String path) {
    if (path == null || path.isEmpty()) {
      return "/";
    }
    return path.startsWith("/") ? path : "/" + path;
  }

  private static String queryString(Map<String, String> query) {
    if (query == null || query.isEmpty()) {
      return "";
    }
    StringJoiner joiner = new StringJoiner("&", "?", "");
    boolean any = false;
    for (Map.Entry<String, String> entry : query.entrySet()) {
      if (entry.getKey() == null || entry.getValue() == null) {
        continue;
      }
      joiner.add(percentEncode(entry.getKey()) + "=" + percentEncode(entry.getValue()));
      any = true;
    }
    return any ? joiner.toString() : "";
  }

  private static String percentEncode(String value) {
    StringBuilder encoded = new StringBuilder();
    for (byte raw : value.getBytes(StandardCharsets.UTF_8)) {
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

  /**
   * Percent-encodes a single path segment, including any "/". Rejects {@code null}, empty,
   * {@code "."}, and {@code ".."} to prevent path traversal.
   */
  public static String enc(String segment) {
    if (segment == null || segment.isEmpty() || segment.equals(".") || segment.equals("..")) {
      throw new IllegalArgumentException(
          "invalid path segment: " + (segment == null ? "null" : "\"" + segment + "\""));
    }
    return percentEncode(segment);
  }

  /**
   * Percent-encodes each segment of a slash-separated path, keeping the slashes. Strips leading
   * slashes and rejects {@code null}, empty paths, and any empty, {@code "."}, or {@code ".."}
   * segment.
   */
  public static String encPath(String path) {
    if (path == null) {
      throw new IllegalArgumentException("invalid path: null");
    }
    String stripped = path;
    while (stripped.startsWith("/")) {
      stripped = stripped.substring(1);
    }
    if (stripped.isEmpty()) {
      throw new IllegalArgumentException("invalid path: \"" + path + "\"");
    }
    StringJoiner joiner = new StringJoiner("/");
    for (String part : stripped.split("/", -1)) {
      joiner.add(enc(part));
    }
    return joiner.toString();
  }
}
