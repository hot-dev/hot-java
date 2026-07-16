package dev.hot.sdk.internal;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.hot.sdk.HotTransportException;
import java.util.LinkedHashMap;
import java.util.Map;

/** Internal JSON helpers. Not part of the public API. */
public final class Json {

  public static final ObjectMapper MAPPER = new ObjectMapper();

  private Json() {}

  public static String write(Object value) {
    try {
      return MAPPER.writeValueAsString(value);
    } catch (JsonProcessingException error) {
      throw new HotTransportException("failed to encode request body", error);
    }
  }

  public static Object read(String text) {
    try {
      return MAPPER.readValue(text, Object.class);
    } catch (JsonProcessingException error) {
      throw new HotTransportException("invalid JSON response", error);
    }
  }

  /** Parses text as a JSON object, returning null for anything else. */
  @SuppressWarnings("unchecked")
  public static Map<String, Object> tryReadObject(String text) {
    if (text == null || text.isEmpty()) {
      return null;
    }
    try {
      Object parsed = MAPPER.readValue(text, Object.class);
      return parsed instanceof Map<?, ?> map ? (Map<String, Object>) map : null;
    } catch (JsonProcessingException error) {
      return null;
    }
  }

  /**
   * Normalizes a response body into the API envelope shape. Empty bodies become
   * {@code {"data": null, "meta": {}}}; non-object JSON is wrapped the same way.
   */
  @SuppressWarnings("unchecked")
  public static Map<String, Object> envelope(String body) {
    if (body == null || body.isEmpty()) {
      return wrapped(null);
    }
    Object parsed = read(body);
    if (parsed instanceof Map<?, ?> map) {
      return (Map<String, Object>) map;
    }
    return wrapped(parsed);
  }

  private static Map<String, Object> wrapped(Object data) {
    Map<String, Object> envelope = new LinkedHashMap<>();
    envelope.put("data", data);
    envelope.put("meta", new LinkedHashMap<String, Object>());
    return envelope;
  }

  /** Extracts the envelope's "data" member when it is an object. */
  @SuppressWarnings("unchecked")
  public static Map<String, Object> dataObject(Map<String, Object> envelope) {
    return envelope.get("data") instanceof Map<?, ?> data ? (Map<String, Object>) data : null;
  }
}
