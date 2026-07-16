package dev.hot.sdk;

import dev.hot.sdk.internal.Json;
import dev.hot.sdk.internal.Responses;
import dev.hot.sdk.internal.Transport;
import java.net.http.HttpRequest.BodyPublishers;
import java.util.Map;

/** Manages custom domains. */
public final class DomainsResource {

  private final Transport transport;

  DomainsResource(Transport transport) {
    this.transport = transport;
  }

  /** Registers a custom domain. */
  public Map<String, Object> create(Map<String, Object> body) {
    return Json.dataObject(transport.requestJson("POST", "/domains", body, null));
  }

  /** Returns the domain list envelope. */
  public Map<String, Object> list() {
    return transport.requestJson("GET", "/domains", null, null);
  }

  /** Returns a single domain. */
  public Map<String, Object> get(String domainId) {
    return Json.dataObject(
        transport.requestJson("GET", "/domains/" + Transport.enc(domainId), null, null));
  }

  /** Deletes a domain. */
  public void delete(String domainId) {
    Responses.closeQuietly(
        transport.requestRaw(
            "DELETE", "/domains/" + Transport.enc(domainId), null, null,
            BodyPublishers.noBody()));
  }

  /** Triggers domain verification. */
  public Map<String, Object> verify(String domainId) {
    return Json.dataObject(
        transport.requestJson(
            "POST", "/domains/" + Transport.enc(domainId) + "/verify", null, null));
  }
}
