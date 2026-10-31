package dev.hot.sdk.internal;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.hot.sdk.HotTransportException;
import java.util.LinkedHashMap;
import java.util.Map;

/** Internal JSON helpers. Not part of the public API. */
public final class Json {

  public static final ObjectMapper MAPPER = new ObjectMapper();

  // Decodes every JSON number with a fraction or exponent as BigDecimal, so Hot Dec values and
  // user payloads keep all their digits. Integers still decode as Integer/Long/BigInteger.
  private static final ObjectMapper DECIMAL_MAPPER =
      new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

  private Json() {}

  private static ObjectMapper reader(boolean bigDecimals) {
    return bigDecimals ? DECIMAL_MAPPER : MAPPER;
  }

  public static String write(Object value) {
    try {
      return MAPPER.writeValueAsString(value);
    } catch (JsonProcessingException error) {
      throw new HotTransportException("failed to encode request body", error);
    }
  }

  public static Object read(String text) {
    return read(text, false);
  }

  public static Object read(String text, boolean bigDecimals) {
    try {
      return reader(bigDecimals).readValue(text, Object.class);
    } catch (JsonProcessingException error) {
      throw new HotTransportException("invalid JSON response", error);
    }
  }

  /** Parses text as a JSON object, returning null for anything else. */
  public static Map<String, Object> tryReadObject(String text) {
    return tryReadObject(text, false);
  }

  /** Parses text as a JSON object, returning null for anything else. */
  @SuppressWarnings("unchecked")
  public static Map<String, Object> tryReadObject(String text, boolean bigDecimals) {
    if (text == null || text.isEmpty()) {
      return null;
    }
    try {
      Object parsed = reader(bigDecimals).readValue(text, Object.class);
      return parsed instanceof Map<?, ?> map ? (Map<String, Object>) map : null;
    } catch (JsonProcessingException error) {
      return null;
    }
  }

  /**
   * Normalizes a response body into the API envelope shape. Empty bodies become
   * {@code {"data": null, "meta": {}}}; non-object JSON is wrapped the same way.
   */
  public static Map<String, Object> envelope(String body) {
    return envelope(body, false);
  }

  /** {@link #envelope(String)}, optionally decoding floats as BigDecimal. */
  @SuppressWarnings("unchecked")
  public static Map<String, Object> envelope(String body, boolean bigDecimals) {
    if (body == null || body.isEmpty()) {
      return wrapped(null);
    }
    Object parsed = read(body, bigDecimals);
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
