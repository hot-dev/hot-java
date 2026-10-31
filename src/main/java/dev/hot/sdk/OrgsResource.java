package dev.hot.sdk;

import dev.hot.sdk.internal.Json;
import dev.hot.sdk.internal.Transport;
import java.util.Map;

/**
 * Org API: child organizations, their environments, and org and environment API keys. Needs an
 * org API key; other credentials get {@link HotKeyScopeException} with required scope {@code
 * "org"}. An org API key in turn cannot call runtime resources such as {@link
 * HotClient#events()}: mint an environment key with {@link #createEnvApiKey} and use a separate
 * client with it.
 */
public final class OrgsResource {

  private final Transport transport;

  OrgsResource(Transport transport) {
    this.transport = transport;
  }

  /** Returns the list envelope of child organizations the key may read. */
  public Map<String, Object> list() {
    return list(null);
  }

  /** Returns the child organization list envelope, paginated by {@code limit}/{@code offset}. */
  public Map<String, Object> list(Map<String, String> query) {
    return transport.requestJson("GET", "/orgs", null, query);
  }

  /**
   * Creates a child organization ({@code name}, {@code slug} of at most 32 characters, optional
   * {@code external_id}, and a required {@code idempotency_key}; reuse the key when retrying).
   * Returns {@code {org, env_id, created}}: {@code env_id} is the child's default environment and
   * {@code created} is false when the idempotency key matched an earlier request.
   */
  public Map<String, Object> create(Map<String, Object> body) {
    return Json.dataObject(transport.requestJson("POST", "/orgs", body, null));
  }

  /** Returns an organization. */
  public Map<String, Object> get(String orgId) {
    return Json.dataObject(transport.requestJson("GET", org(orgId), null, null));
  }

  /**
   * Changes a child organization's lifecycle: {@code {"state": "active" | "suspended" |
   * "archived"}}. Returns the updated organization.
   */
  public Map<String, Object> update(String orgId, Map<String, Object> body) {
    return Json.dataObject(transport.requestJson("PATCH", org(orgId), body, null));
  }

  /**
   * Returns account usage for the current billing period, across the root and every child. Pass
   * the root organization's id (the org key's own organization).
   */
  public Map<String, Object> usage(String rootOrgId) {
    return Json.dataObject(transport.requestJson("GET", org(rootOrgId) + "/usage", null, null));
  }

  /** Deletes every call trace in the organization's environments. */
  public Map<String, Object> purgeCalls(String orgId) {
    return purgeCalls(orgId, false);
  }

  /**
   * Deletes every call trace in the organization's environments, and in its children's with
   * {@code includeSubOrgs}. Returns {@code {calls_deleted, blob_refs_released, complete}}; call
   * again while {@code complete} is false.
   */
  public Map<String, Object> purgeCalls(String orgId, boolean includeSubOrgs) {
    Map<String, String> query = includeSubOrgs ? Map.of("include_sub_orgs", "true") : null;
    return Json.dataObject(transport.requestJson("DELETE", org(orgId) + "/calls", null, query));
  }

  /** Returns the list envelope of an organization's environments. */
  public Map<String, Object> listEnvs(String orgId) {
    return transport.requestJson("GET", org(orgId) + "/envs", null, null);
  }

  /** Creates an environment ({@code {"name": ...}}); idempotent by exact name. */
  public Map<String, Object> createEnv(String orgId, Map<String, Object> body) {
    return Json.dataObject(transport.requestJson("POST", org(orgId) + "/envs", body, null));
  }

  /** Returns an environment. */
  public Map<String, Object> getEnv(String orgId, String envId) {
    return Json.dataObject(transport.requestJson("GET", envPath(orgId, envId), null, null));
  }

  /** Returns the list envelope of the organization's org API keys (the key's own org only). */
  public Map<String, Object> listApiKeys(String orgId) {
    return listApiKeys(orgId, null);
  }

  /** Returns the org API key list envelope, filtered by query parameters. */
  public Map<String, Object> listApiKeys(String orgId, Map<String, String> query) {
    return transport.requestJson("GET", org(orgId) + "/api-keys", null, query);
  }

  /**
   * Mints an org API key ({@code description}, {@code permissions}, optional {@code
   * expires_at}). Its grants must fit inside the caller's. The response carries the {@code token}
   * once.
   */
  public Map<String, Object> createApiKey(String orgId, Map<String, Object> body) {
    return Json.dataObject(transport.requestJson("POST", org(orgId) + "/api-keys", body, null));
  }

  /** Returns the list envelope of an environment's API keys. */
  public Map<String, Object> listEnvApiKeys(String orgId, String envId) {
    return listEnvApiKeys(orgId, envId, null);
  }

  /** Returns an environment's API key list envelope, filtered by query parameters. */
  public Map<String, Object> listEnvApiKeys(
      String orgId, String envId, Map<String, String> query) {
    return transport.requestJson("GET", envPath(orgId, envId) + "/api-keys", null, query);
  }

  /**
   * Mints an environment API key ({@code description}, {@code permissions} in the environment
   * grammar such as {@code {"event:*": ["create"]}}, optional {@code expires_at}). Its grants must
   * fit inside the org key's grants for that environment. The response carries the {@code token}
   * once; use it with a separate {@link HotClient} for runtime calls.
   */
  public Map<String, Object> createEnvApiKey(
      String orgId, String envId, Map<String, Object> body) {
    return Json.dataObject(
        transport.requestJson("POST", envPath(orgId, envId) + "/api-keys", body, null));
  }

  /**
   * Projects, builds and context inside one environment, authorized by this org key's grants for
   * it.
   */
  public OrgEnv env(String orgId, String envId) {
    return new OrgEnv(transport, envPath(orgId, envId));
  }

  private static String org(String orgId) {
    return "/orgs/" + Transport.enc(orgId);
  }

  private static String envPath(String orgId, String envId) {
    return org(orgId) + "/envs/" + Transport.enc(envId);
  }
}
