package dev.hot.sdk.internal;

import dev.hot.sdk.HotTransportException;
import java.io.IOException;
import java.io.InputStream;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/** Helpers for raw HTTP responses. Not part of the public API. */
public final class Responses {

  private Responses() {}

  /** Reads a raw JSON response and returns its "data" object. */
  public static Map<String, Object> readData(HttpResponse<InputStream> response) {
    return readData(response, false);
  }

  /** Reads a raw JSON response and returns its "data" object, optionally with BigDecimals. */
  public static Map<String, Object> readData(
      HttpResponse<InputStream> response, boolean bigDecimals) {
    try (InputStream stream = response.body()) {
      String body = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
      return Json.dataObject(Json.envelope(body, bigDecimals));
    } catch (IOException error) {
      throw new HotTransportException(error);
    }
  }

  /** Closes a response body, ignoring close failures on already-successful requests. */
  public static void closeQuietly(HttpResponse<InputStream> response) {
    try {
      response.body().close();
    } catch (IOException ignored) {
      // The request already succeeded; a close failure changes nothing.
    }
  }
}
