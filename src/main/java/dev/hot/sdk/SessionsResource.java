package dev.hot.sdk;

import dev.hot.sdk.internal.Json;
import dev.hot.sdk.internal.Responses;
import dev.hot.sdk.internal.Transport;
import java.net.http.HttpRequest.BodyPublishers;
import java.util.Map;

/** Creates and revokes scoped sessions. */
public final class SessionsResource {

  private final Transport transport;

  SessionsResource(Transport transport) {
    this.transport = transport;
  }

  /** Creates a scoped session. */
  public Map<String, Object> create(Map<String, Object> body) {
    return Json.dataObject(transport.requestJson("POST", "/sessions", body, null));
  }

  /** Returns the session list envelope. */
  public Map<String, Object> list() {
    return list(null);
  }

  /** Returns the session list envelope filtered by query parameters. */
  public Map<String, Object> list(Map<String, String> query) {
    return transport.requestJson("GET", "/sessions", null, query);
  }

  /** Revokes one session. */
  public void revoke(String sessionId) {
    Responses.closeQuietly(
        transport.requestRaw(
            "DELETE", "/sessions/" + Transport.enc(sessionId), null, null,
            BodyPublishers.noBody()));
  }

  /** Revokes every session. */
  public Map<String, Object> revokeAll() {
    return Json.dataObject(transport.requestJson("DELETE", "/sessions", null, null));
  }
}
