package dev.hot.sdk;

import dev.hot.sdk.internal.Json;
import java.net.http.HttpHeaders;
import java.util.Map;
import java.util.Optional;

/** Structured exception thrown for non-2xx Hot API responses. */
public final class HotApiException extends RuntimeException {

  private final int statusCode;
  private final String code;
  private final String requestId;
  private final Integer retryAfter;
  private final Map<String, Object> errorBody;

  public HotApiException(
      int statusCode, String message, String code, String requestId, Integer retryAfter,
      Map<String, Object> errorBody) {
    super(message);
    this.statusCode = statusCode;
    this.code = code;
    this.requestId = requestId;
    this.retryAfter = retryAfter;
    this.errorBody = errorBody;
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
    Map<String, Object> parsed = Json.tryReadObject(body);
    Object error = parsed == null ? null : parsed.get("error");
    if (error instanceof Map<?, ?> errorMap) {
      @SuppressWarnings("unchecked")
      Map<String, Object> errorBody = (Map<String, Object>) errorMap;
      Object message = errorBody.get("message");
      return new HotApiException(
          statusCode,
          message instanceof String text && !text.isEmpty()
              ? text
              : "Hot API error (" + statusCode + ")",
          stringified(errorBody.get("code")),
          stringified(errorBody.get("request_id")),
          retryAfter(errorBody.get("retry_after"), headers),
          errorBody);
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
