package dev.hot.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import org.junit.jupiter.api.Test;

/** Pure unit tests for validation, nullability, error parsing, and client configuration. */
class SecurityAndValidationTest {

  private static HttpHeaders headers(String name, String value) {
    return HttpHeaders.of(Map.of(name, List.of(value)), (k, v) -> true);
  }

  @Test
  void pathValidationRejectsTraversalLocallyBeforeAnyHttpCall() {
    // Unreachable port 1: if any HTTP call were attempted, HotTransportException would be thrown.
    HotClient client = HotClient.builder("test-token").baseUrl("http://127.0.0.1:1").build();
    byte[] bytes = "hi".getBytes(StandardCharsets.UTF_8);

    assertThrows(IllegalArgumentException.class, () -> client.sessions().revoke("."));
    assertThrows(IllegalArgumentException.class, () -> client.projects().get(".."));
    assertThrows(IllegalArgumentException.class, () -> client.projects().get(""));
    assertThrows(IllegalArgumentException.class, () -> client.projects().get(null));
    assertThrows(IllegalArgumentException.class, () -> client.files().upload("../x", bytes));
    assertThrows(IllegalArgumentException.class, () -> client.files().upload("a/./b", bytes));
    assertThrows(IllegalArgumentException.class, () -> client.files().upload("a//b", bytes));
    assertThrows(IllegalArgumentException.class, () -> client.files().upload("", bytes));
    assertThrows(IllegalArgumentException.class, () -> client.files().upload("/", bytes));
    assertThrows(IllegalArgumentException.class, () -> client.files().upload(null, bytes));
  }

  @Test
  void hotClientBuilderValidatesTimeoutRedirectsNullBaseUrlAndAutoCloseable() {
    assertThrows(
        IllegalArgumentException.class,
        () -> HotClient.builder("tok").timeout(Duration.ZERO).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> HotClient.builder("tok").timeout(Duration.ofSeconds(-1)).build());

    HttpClient redirecting =
        HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).build();
    assertThrows(
        IllegalArgumentException.class,
        () -> HotClient.builder("tok").httpClient(redirecting));

    try (HotClient client = HotClient.builder("tok").baseUrl(null).build()) {
      assertEquals("https://api.hot.dev", client.baseUrl());
    }
    try (HotClient client = HotClient.builder("tok").baseUrl("https://api.hot.dev/v1/").build()) {
      assertEquals("https://api.hot.dev", client.baseUrl());
    }
  }

  @Test
  void optionsValidatePositiveTimeoutAndHandleNullTimeout() {
    assertThrows(
        IllegalArgumentException.class, () -> new CallOptions().timeout(Duration.ZERO));
    assertThrows(
        IllegalArgumentException.class, () -> new WaitOptions().timeout(Duration.ofSeconds(-1)));
    assertThrows(
        IllegalArgumentException.class, () -> new RunWaitOptions().timeout(Duration.ZERO));
    assertThrows(
        IllegalArgumentException.class, () -> new TaskWaitOptions().timeout(Duration.ofMillis(-5)));

    assertEquals(Duration.ofSeconds(60), new CallOptions().timeout(null).timeout());
    assertEquals(Duration.ofSeconds(60), new WaitOptions().timeout(null).timeout());
    assertEquals(Duration.ofSeconds(60), new RunWaitOptions().timeout(null).timeout());
    assertEquals(Duration.ofSeconds(60), new TaskWaitOptions().timeout(null).timeout());
  }

  @Test
  void buildUploadValidatesContentHashAndFilename() {
    byte[] bytes = "archive".getBytes(StandardCharsets.UTF_8);
    assertThrows(IllegalArgumentException.class, () -> new BuildUpload(null, "sha256"));
    assertThrows(IllegalArgumentException.class, () -> new BuildUpload(bytes, null));
    assertThrows(IllegalArgumentException.class, () -> new BuildUpload(bytes, ""));
    assertThrows(IllegalArgumentException.class, () -> new BuildUpload(bytes, "sha256").filename(null));
    assertThrows(IllegalArgumentException.class, () -> new BuildUpload(bytes, "sha256").filename(""));

    BuildUpload ok = new BuildUpload(bytes, "sha256").filename("app.tar.gz").buildId("b1");
    assertEquals("app.tar.gz", ok.filename());
    assertEquals("b1", ok.buildId());
  }

  @Test
  void hotApiExceptionParsesRetryAfterTrimClampCeilAndHttpDate() {
    // Trimmed integer
    HotApiException trimmed =
        HotApiException.parse(429, "slow", headers("Retry-After", "  7  "), "/events");
    assertEquals(7, trimmed.retryAfter());

    // Negative header clamped to 0
    HotApiException negHeader =
        HotApiException.parse(429, "slow", headers("Retry-After", "-12"), "/events");
    assertEquals(0, negHeader.retryAfter());

    // Negative JSON retry_after clamped to 0
    HotApiException negJson =
        HotApiException.parse(
            429, "{\"error\":{\"code\":\"rate_limited\",\"message\":\"no\",\"retry_after\":-3}}", null);
    assertEquals(0, negJson.retryAfter());

    // Sub-second float JSON retry_after ceiled to 1
    HotApiException floatJson =
        HotApiException.parse(
            429,
            "{\"error\":{\"code\":\"rate_limited\",\"message\":\"no\",\"retry_after\":0.5}}",
            null);
    assertEquals(1, floatJson.retryAfter());

    // Fractional header ceiled
    HotApiException floatHeader =
        HotApiException.parse(429, "slow", headers("Retry-After", "1.2"), "/events");
    assertEquals(2, floatHeader.retryAfter());

    // Future RFC 9110 HTTP-date
    String futureDate =
        DateTimeFormatter.RFC_1123_DATE_TIME.format(
            Instant.now().plusSeconds(15).atOffset(ZoneOffset.UTC));
    HotApiException future =
        HotApiException.parse(503, "busy", headers("Retry-After", " " + futureDate + " "));
    assertTrue(
        future.retryAfter() != null && future.retryAfter() >= 10 && future.retryAfter() <= 20,
        "retryAfter=" + future.retryAfter());

    // Past RFC 9110 HTTP-date clamped to 0
    String pastDate =
        DateTimeFormatter.RFC_1123_DATE_TIME.format(
            Instant.now().minusSeconds(60).atOffset(ZoneOffset.UTC));
    HotApiException past = HotApiException.parse(503, "busy", headers("Retry-After", pastDate));
    assertEquals(0, past.retryAfter());

    // Invalid Retry-After returns null
    HotApiException garbage =
        HotApiException.parse(503, "busy", headers("Retry-After", "not-a-date"));
    assertNull(garbage.retryAfter());
  }

  @Test
  void hotApiExceptionErrorBodyIsUnmodifiableAndHugeFallbackMessageIsBounded() {
    HotApiException parsed =
        HotApiException.parse(
            400,
            "{\"error\":{\"code\":\"bad_request\",\"message\":\"bad\",\"detail\":\"x\"}}",
            null);
    assertThrows(
        UnsupportedOperationException.class, () -> parsed.errorBody().put("mutated", true));

    String huge = "<html>" + "a".repeat(20_000) + "</html>";
    HotApiException html = HotApiException.parse(502, huge, null);
    assertTrue(html.getMessage().length() <= 4100, "length=" + html.getMessage().length());
    assertTrue(html.getMessage().endsWith("..."));
  }

  @Test
  void hotKeyScopeExceptionRequiredScopeHandlesV1AndBarePaths() {
    assertEquals("org", HotKeyScopeException.requiredScope("/orgs", ""));
    assertEquals("org", HotKeyScopeException.requiredScope("/orgs/o1", ""));
    assertEquals("org", HotKeyScopeException.requiredScope("orgs", ""));
    assertEquals("org", HotKeyScopeException.requiredScope("orgs/o1", ""));
    assertEquals("org", HotKeyScopeException.requiredScope("/v1/orgs", ""));
    assertEquals("org", HotKeyScopeException.requiredScope("/v1/orgs/o1", ""));
    assertEquals("org", HotKeyScopeException.requiredScope("/api-keys/k1", ""));
    assertEquals("org", HotKeyScopeException.requiredScope("api-keys/k1", ""));
    assertEquals("org", HotKeyScopeException.requiredScope("/v1/api-keys/k1", ""));

    assertEquals("env", HotKeyScopeException.requiredScope("/org/usage", ""));
    assertEquals("env", HotKeyScopeException.requiredScope("/v1/org/usage", ""));
    assertEquals("env", HotKeyScopeException.requiredScope("/env", ""));
    assertEquals("env", HotKeyScopeException.requiredScope("/organizations", ""));
  }

  @Test
  void streamEventsRetryRunIdOfExtractsField() {
    assertEquals("rB", StreamEvents.retryRunIdOf(Map.of("type", "run:fail", "retry_run_id", "rB")));
    assertNull(StreamEvents.retryRunIdOf(Map.of("type", "run:fail")));
    assertNull(StreamEvents.retryRunIdOf(Map.of("type", "run:fail", "retry_run_id", "")));
    assertNull(StreamEvents.retryRunIdOf(null));
  }

  @Test
  void asyncHotClientDefaultExecutorIsBoundedAndScalesDown() {
    ThreadPoolExecutor pool = AsyncHotClient.defaultExecutor();
    assertTrue(pool.getMaximumPoolSize() <= 64, "max=" + pool.getMaximumPoolSize());
    assertTrue(pool.allowsCoreThreadTimeOut());
    assertThrows(IllegalArgumentException.class, () -> AsyncHotClient.wrap(null));
    assertThrows(
        IllegalArgumentException.class,
        () -> AsyncHotClient.wrap(HotClient.builder("tok").build(), null));
  }
}
