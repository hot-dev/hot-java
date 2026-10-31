package dev.hot.sdk;

import dev.hot.sdk.internal.Json;
import dev.hot.sdk.internal.Transport;
import java.util.Map;

/**
 * Org and environment API keys by id ({@code /v1/api-keys}). Needs an org API key. Changing,
 * rotating or revoking a key also needs descent: the caller issued it, directly or through keys
 * it issued.
 */
public final class ApiKeysResource {

  private final Transport transport;

  ApiKeysResource(Transport transport) {
    this.transport = transport;
  }

  /** Returns an API key's metadata ({@code scope} is {@code "org"} or {@code "env"}). */
  public Map<String, Object> get(String keyId) {
    return Json.dataObject(transport.requestJson("GET", key(keyId), null, null));
  }

  /**
   * Replaces a key's grants ({@code {"permissions": ...}}); they must fit inside the caller's.
   * Keys the key issued are narrowed with it.
   */
  public Map<String, Object> update(String keyId, Map<String, Object> body) {
    return Json.dataObject(transport.requestJson("PATCH", key(keyId), body, null));
  }

  /** Revokes a key. Everything it issued stops on its next request. */
  public void revoke(String keyId) {
    transport.requestJson("DELETE", key(keyId), null, null);
  }

  /** Issues a new secret with the default overlap (the old secret works for 24 hours). */
  public Map<String, Object> rotate(String keyId) {
    return rotate(keyId, null);
  }

  /**
   * Issues a new secret for the same key; {@code {"overlap_seconds": n}} (0 to 604800) sets how
   * long the previous secret keeps working. The response carries the new {@code token} and
   * {@code previous_secret_expires_at}.
   */
  public Map<String, Object> rotate(String keyId, Map<String, Object> body) {
    return Json.dataObject(transport.requestJson("POST", key(keyId) + "/rotate", body, null));
  }

  private static String key(String keyId) {
    return "/api-keys/" + Transport.enc(keyId);
  }
}
