package dev.hot.sdk;

import dev.hot.sdk.internal.Json;
import dev.hot.sdk.internal.Transport;
import java.util.Map;

/** Reads environment info and environment events. */
public final class EnvResource {

  private final Transport transport;

  EnvResource(Transport transport) {
    this.transport = transport;
  }

  /** Returns environment info. */
  public Map<String, Object> get() {
    return Json.dataObject(transport.requestJson("GET", "/env", null, null));
  }

  /**
   * Streams environment events. Requires API key credentials and a live API pub/sub backend.
   */
  public StreamEvents subscribe() {
    return transport.stream("GET", "/env/subscribe", null, null);
  }
}
