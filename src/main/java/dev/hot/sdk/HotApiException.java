package dev.hot.sdk;

import dev.hot.sdk.internal.Json;
import java.net.http.HttpHeaders;
import java.util.Map;
import java.util.Optional;

/**
 * Structured exception thrown for non-2xx Hot API responses. A {@code 403 key_scope_mismatch} is
 * the subclass {@link HotKeyScopeException}.
 */
public class HotApiException extends RuntimeException {

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
    this.errorBody = errorBody;
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
        body == null || body.isEmpty() ? "Hot API error (" + statusCode + ")" : body,
        null,
        null,
        retryAfter(null, headers),
        null);
  }

  private static String stringified(Object value) {
    return value == null ? null : String.valueOf(value);
  }

  private static Integer retryAfter(Object value, HttpHeaders headers) {
    if (value instanceof Number number) {
      return number.intValue();
    }
    Optional<String> header = headers.firstValue("retry-after");
    if (header.isPresent()) {
      try {
        return Integer.parseInt(header.get());
      } catch (NumberFormatException ignored) {
        return null;
      }
    }
    return null;
  }
}
