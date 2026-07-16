package dev.hot.sdk;

import dev.hot.sdk.internal.Json;
import dev.hot.sdk.internal.Transport;
import java.util.Map;

/** Reports organization usage and limits. */
public final class OrgResource {

  private final Transport transport;

  OrgResource(Transport transport) {
    this.transport = transport;
  }

  /** Returns organization usage and limits. */
  public Map<String, Object> usage() {
    return Json.dataObject(transport.requestJson("GET", "/org/usage", null, null));
  }
}
