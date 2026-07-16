package dev.hot.sdk;

import dev.hot.sdk.internal.Json;
import dev.hot.sdk.internal.Transport;
import java.util.Map;

/** Lists and inspects runs. */
public final class RunsResource {

  private final Transport transport;

  RunsResource(Transport transport) {
    this.transport = transport;
  }

  /** Returns the run list envelope (data + pagination). */
  public Map<String, Object> list() {
    return list(null);
  }

  /** Returns the run list envelope filtered by query parameters. */
  public Map<String, Object> list(Map<String, String> query) {
    return transport.requestJson("GET", "/runs", null, query);
  }

  /** Returns aggregate run statistics. */
  public Map<String, Object> stats() {
    return Json.dataObject(transport.requestJson("GET", "/runs/stats", null, null));
  }

  /** Returns a single run. */
  public Map<String, Object> get(String runId) {
    return Json.dataObject(
        transport.requestJson("GET", "/runs/" + Transport.enc(runId), null, null));
  }
}
