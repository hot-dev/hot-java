package dev.hot.sdk;

import dev.hot.sdk.internal.Json;
import dev.hot.sdk.internal.Responses;
import dev.hot.sdk.internal.Transport;
import java.net.http.HttpRequest.BodyPublishers;
import java.util.Map;

/** Creates and revokes scoped service keys. */
public final class ServiceKeysResource {

  private final Transport transport;

  ServiceKeysResource(Transport transport) {
    this.transport = transport;
  }

  /** Creates a scoped service key. */
  public Map<String, Object> create(Map<String, Object> body) {
    return Json.dataObject(transport.requestJson("POST", "/service-keys", body, null));
  }

  /** Returns the service key list envelope. */
  public Map<String, Object> list() {
    return list(null);
  }

  /** Returns the service key list envelope filtered by query parameters. */
  public Map<String, Object> list(Map<String, String> query) {
    return transport.requestJson("GET", "/service-keys", null, query);
  }

  /** Returns a single service key. */
  public Map<String, Object> get(String serviceKeyId) {
    return Json.dataObject(
        transport.requestJson("GET", "/service-keys/" + Transport.enc(serviceKeyId), null, null));
  }

  /** Patches a service key. */
  public Map<String, Object> update(String serviceKeyId, Map<String, Object> body) {
    return Json.dataObject(
        transport.requestJson(
            "PATCH", "/service-keys/" + Transport.enc(serviceKeyId), body, null));
  }

  /** Revokes one service key. */
  public void revoke(String serviceKeyId) {
    Responses.closeQuietly(
        transport.requestRaw(
            "DELETE", "/service-keys/" + Transport.enc(serviceKeyId), null, null,
            BodyPublishers.noBody()));
  }

  /** Revokes every service key. */
  public Map<String, Object> revokeAll() {
    return Json.dataObject(transport.requestJson("DELETE", "/service-keys", null, null));
  }
}
