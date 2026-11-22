package dev.hot.sdk;

import dev.hot.sdk.internal.Json;
import java.net.http.HttpHeaders;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Structured exception thrown for non-2xx Hot API responses. A {@code 403 key_scope_mismatch} is
 * the subclass {@link HotKeyScopeException}.
 */
public class HotApiException extends RuntimeException {

  private static final int MAX_FALLBACK_MESSAGE_CHARS = 4096;

  private final int statusCode;
  private final String code;
  private final String requestId;
  private final Integer retryAfter;
  private final Map<String, Object> errorBody;

  public HotApiException(
      int statusCode,
      String message,
      String code,
      String requestId,
      Integer retryAfter,
      Map<String, Object> errorBody) {
    super(message);
    this.statusCode = statusCode;
    this.code = code;
    this.requestId = requestId;
    this.retryAfter = retryAfter;
    this.errorBody =
        errorBody == null
            ? null
            : Collections.unmodifiableMap(new LinkedHashMap<>(errorBody));
  }

  public boolean isAuthorizationFailure() {
    return statusCode == 401 || statusCode == 403;
  }

  /**
   * True for errors a retry cannot fix: every 4xx except 408 and 429. Durable waiters stop on
   * these instead of reconnecting.
   */
  public boolean isPermanent() {
    return statusCode >= 400 && statusCode < 500 && statusCode != 408 && statusCode != 429;
  }

  public int statusCode() {
    return statusCode;
  }

  /** The error code from the response body, or null. */
  public String code() {
    return code;
  }

  /** The request id from the response body, or null. */
  public String requestId() {
    return requestId;
  }

  /**
   * Server-suggested retry delay in seconds, from the error body or Retry-After header, or null.
   */
  public Integer retryAfter() {
    return retryAfter;
  }

  /** The raw "error" object from the response body, or null. */
  public Map<String, Object> errorBody() {
    return errorBody;
  }

  /** Parses an API error response into a structured exception. */
  public static HotApiException parse(int statusCode, String body, HttpHeaders headers) {
    return parse(statusCode, body, headers, null);
  }

  /**
   * Parses an API error response for a request to {@code path} (relative to /v1, or null). The
   * path tells a {@link HotKeyScopeException} which key scope the call needed.
   */
  public static HotApiException parse(
      int statusCode, String body, HttpHeaders headers, String path) {
    Map<String, Object> parsed = Json.tryReadObject(body);
    Object error = parsed == null ? null : parsed.get("error");
    if (error instanceof Map<?, ?> errorMap) {
      @SuppressWarnings("unchecked")
      Map<String, Object> errorBody = (Map<String, Object>) errorMap;
      Object message = errorBody.get("message");
      String text =
          message instanceof String value && !value.isEmpty()
              ? value
              : "Hot API error (" + statusCode + ")";
      String code = stringified(errorBody.get("code"));
      String requestId = stringified(errorBody.get("request_id"));
      Integer retryAfter = retryAfter(errorBody.get("retry_after"), headers);
      if (HotKeyScopeException.CODE.equals(code)) {
        return new HotKeyScopeException(
            statusCode, text, requestId, errorBody, HotKeyScopeException.requiredScope(path, text));
      }
      return new HotApiException(statusCode, text, code, requestId, retryAfter, errorBody);
    }
    return new HotApiException(
        statusCode,
        fallbackMessage(statusCode, body),
        null,
        null,
        retryAfter(null, headers),
        null);
  }

  private static String fallbackMessage(int statusCode, String body) {
    if (body == null || body.isBlank()) {
      return "Hot API error (" + statusCode + ")";
    }
    if (body.length() > MAX_FALLBACK_MESSAGE_CHARS) {
      return body.substring(0, MAX_FALLBACK_MESSAGE_CHARS) + "...";
    }
    return body;
  }

  private static String stringified(Object value) {
    return value == null ? null : String.valueOf(value);
  }

  private static Integer retryAfter(Object value, HttpHeaders headers) {
    if (value instanceof Number number) {
      double raw = number.doubleValue();
      if (Double.isNaN(raw) || raw <= 0.0) {
        return 0;
      }
      double ceiled = Math.ceil(raw);
      return ceiled >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) ceiled;
    }
    Optional<String> header =
        headers == null ? Optional.empty() : headers.firstValue("retry-after");
    if (header.isPresent()) {
      String raw = header.get().trim();
      if (raw.isEmpty()) {
        return null;
      }
      try {
        long seconds = Long.parseLong(raw);
        return (int) Math.min(Math.max(seconds, 0L), Integer.MAX_VALUE);
      } catch (NumberFormatException ignored) {
        // Fall through to float seconds or RFC 9110 HTTP-date parsing.
      }
      try {
        double seconds = Double.parseDouble(raw);
        if (!Double.isNaN(seconds) && !Double.isInfinite(seconds)) {
          if (seconds <= 0.0) {
            return 0;
          }
          double ceiled = Math.ceil(seconds);
          return ceiled >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) ceiled;
        }
      } catch (NumberFormatException ignored) {
        // Fall through to RFC 9110 HTTP-date parsing.
      }
      try {
        ZonedDateTime target = ZonedDateTime.parse(raw, DateTimeFormatter.RFC_1123_DATE_TIME);
        long millis = Duration.between(Instant.now(), target.toInstant()).toMillis();
        if (millis <= 0L) {
          return 0;
        }
        long seconds = (millis + 999L) / 1000L;
        return (int) Math.min(seconds, Integer.MAX_VALUE);
      } catch (DateTimeParseException ignored) {
        return null;
      }
    }
    return null;
  }
}
