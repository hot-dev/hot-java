package dev.hot.sdk;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.anyRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Paths, methods and bodies of the org API resources, and the typed key-scope error. */
class OrgsTest {

  private WireMockServer server;

  @BeforeEach
  void start() {
    // HTTP/1.1 only, as the Hot API serves plain HTTP: the JDK 17 client
    // drops reused h2c connections mid-retry.
    server = new WireMockServer(options().dynamicPort().http2PlainDisabled(true));
    server.start();
    server.stubFor(
        any(anyUrl())
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody("{\"data\":{\"ok\":true},\"meta\":{}}")));
  }

  @AfterEach
  void stop() {
    server.stop();
  }

  private HotClient client() {
    return HotClient.builder("hot_org").baseUrl(server.baseUrl()).build();
  }

  /** Runs {@code call} and returns the single request it made. */
  private LoggedRequest only(Consumer<HotClient> call) {
    server.resetRequests();
    call.accept(client());
    List<LoggedRequest> requests = server.findAll(anyRequestedFor(anyUrl()));
    assertEquals(1, requests.size());
    return requests.get(0);
  }

  private void expect(String method, String url, Object body, Consumer<HotClient> call)
      throws Exception {
    LoggedRequest request = only(call);
    assertEquals(method, request.getMethod().getName());
    assertEquals(url, request.getUrl());
    if (body == null) {
      assertTrue(request.getBodyAsString().isEmpty(), request.getBodyAsString());
    } else {
      assertEquals(body, new ObjectMapper().readValue(request.getBodyAsString(), Object.class));
    }
  }

  @Test
  void orgsMethodsUseTheOrgRoutes() throws Exception {
    Map<String, Object> create =
        Map.of("name", "Acme", "slug", "acme", "idempotency_key", "acme-1");
    expect("GET", "/v1/orgs", null, c -> c.orgs().list());
    expect("GET", "/v1/orgs?limit=5", null, c -> c.orgs().list(Map.of("limit", "5")));
    expect("POST", "/v1/orgs", create, c -> c.orgs().create(create));
    expect("GET", "/v1/orgs/o1", null, c -> c.orgs().get("o1"));
    expect(
        "PATCH",
        "/v1/orgs/o1",
        Map.of("state", "suspended"),
        c -> c.orgs().update("o1", Map.of("state", "suspended")));
    expect("GET", "/v1/orgs/root/usage", null, c -> c.orgs().usage("root"));
    expect("DELETE", "/v1/orgs/o1/calls", null, c -> c.orgs().purgeCalls("o1"));
    expect(
        "DELETE",
        "/v1/orgs/o1/calls?include_sub_orgs=true",
        null,
        c -> c.orgs().purgeCalls("o1", true));
    expect("GET", "/v1/orgs/o1/envs", null, c -> c.orgs().listEnvs("o1"));
    expect(
        "POST",
        "/v1/orgs/o1/envs",
        Map.of("name", "production"),
        c -> c.orgs().createEnv("o1", Map.of("name", "production")));
    expect("GET", "/v1/orgs/o1/envs/e1", null, c -> c.orgs().getEnv("o1", "e1"));
    Map<String, Object> key =
        Map.of("description", "d", "permissions", Map.of("event:*", List.of("create")));
    expect("GET", "/v1/orgs/root/api-keys", null, c -> c.orgs().listApiKeys("root"));
    expect("POST", "/v1/orgs/root/api-keys", key, c -> c.orgs().createApiKey("root", key));
    expect("GET", "/v1/orgs/o1/envs/e1/api-keys", null, c -> c.orgs().listEnvApiKeys("o1", "e1"));
    expect(
        "POST",
        "/v1/orgs/o1/envs/e1/api-keys",
        key,
        c -> c.orgs().createEnvApiKey("o1", "e1", key));
  }

  @Test
  void orgEnvScopesProjectsBuildsAndContext() throws Exception {
    expect(
        "GET", "/v1/orgs/o1/envs/e1/projects", null, c -> c.orgs().env("o1", "e1").projects().list());
    expect(
        "POST",
        "/v1/orgs/o1/envs/e1/projects",
        Map.of("name", "p"),
        c -> c.orgs().env("o1", "e1").projects().create(Map.of("name", "p")));
    expect(
        "POST",
        "/v1/orgs/o1/envs/e1/projects/p/activate",
        null,
        c -> c.orgs().env("o1", "e1").projects().activate("p"));
    expect(
        "GET", "/v1/orgs/o1/envs/e1/builds", null, c -> c.orgs().env("o1", "e1").builds().list());
    expect(
        "GET",
        "/v1/orgs/o1/envs/e1/projects/p/builds",
        null,
        c -> c.orgs().env("o1", "e1").builds().listForProject("p"));
    expect(
        "POST",
        "/v1/orgs/o1/envs/e1/projects/p/builds/b1/deploy",
        null,
        c -> c.orgs().env("o1", "e1").builds().deploy("p", "b1"));
    expect(
        "PUT",
        "/v1/orgs/o1/envs/e1/projects/p/context/a.b",
        Map.of("value", "v"),
        c -> c.orgs().env("o1", "e1").context().update("p", "a.b", Map.of("value", "v")));
  }

  @Test
  void apiKeysAndPurgeMethods() throws Exception {
    expect("GET", "/v1/api-keys/k1", null, c -> c.apiKeys().get("k1"));
    expect(
        "PATCH",
        "/v1/api-keys/k1",
        Map.of("permissions", Map.of()),
        c -> c.apiKeys().update("k1", Map.of("permissions", Map.of())));
    expect("DELETE", "/v1/api-keys/k1", null, c -> c.apiKeys().revoke("k1"));
    expect("POST", "/v1/api-keys/k1/rotate", null, c -> c.apiKeys().rotate("k1"));
    expect(
        "POST",
        "/v1/api-keys/k1/rotate",
        Map.of("overlap_seconds", 0),
        c -> c.apiKeys().rotate("k1", Map.of("overlap_seconds", 0)));
    expect("DELETE", "/v1/env/calls", null, c -> c.env().purgeCalls());
    expect("DELETE", "/v1/runs/r1/calls", null, c -> c.runs().purgeCalls("r1"));
    expect("DELETE", "/v1/streams/s1/calls", null, c -> c.streams().purgeCalls("s1"));
  }

  private void scopeMismatch(String path, String message) {
    server.stubFor(
        any(urlPathEqualTo(path))
            .willReturn(
                aResponse()
                    .withStatus(403)
                    .withHeader("Content-Type", "application/json")
                    .withBody(
                        "{\"error\":{\"code\":\"key_scope_mismatch\",\"message\":\""
                            + message
                            + "\",\"request_id\":\"req_1\"}}")));
  }

  @Test
  void orgKeyOnARuntimeRouteNeedsAnEnvKey() {
    scopeMismatch(
        "/v1/env",
        "This route needs an environment API key; org API keys call /v1/orgs and /v1/api-keys");

    HotKeyScopeException error =
        assertThrows(HotKeyScopeException.class, () -> client().env().get());

    assertEquals("env", error.requiredScope());
    assertEquals(403, error.statusCode());
    assertEquals("key_scope_mismatch", error.code());
    assertEquals("req_1", error.requestId());
    assertTrue(error.getMessage().contains("environment API key"), error.getMessage());
    assertTrue(error.isPermanent());
  }

  @Test
  void envKeyOnAnOrgRouteNeedsAnOrgKey() {
    scopeMismatch("/v1/orgs", "This route needs an org API key; environment keys call runtime");
    scopeMismatch("/v1/api-keys/k1", "denied");

    HotKeyScopeException orgs =
        assertThrows(HotKeyScopeException.class, () -> client().orgs().list());
    HotKeyScopeException keys =
        assertThrows(HotKeyScopeException.class, () -> client().apiKeys().get("k1"));

    assertEquals("org", orgs.requiredScope());
    assertEquals("org", keys.requiredScope());
    assertTrue(keys.getMessage().startsWith("This call needs an org API key"), keys.getMessage());
  }

  @Test
  void keyScopeMismatchOnAStreamStopsWaiters() {
    scopeMismatch("/v1/runs/r1/subscribe", "This route needs an environment API key");

    HotKeyScopeException error =
        assertThrows(HotKeyScopeException.class, () -> client().runs().waitFor("r1"));

    assertEquals("env", error.requiredScope());
    assertEquals(1, server.findAll(anyRequestedFor(urlPathEqualTo("/v1/runs/r1/subscribe"))).size());
  }

  @Test
  void otherErrorsStayPlainApiExceptions() {
    server.stubFor(
        any(urlPathEqualTo("/v1/env"))
            .willReturn(
                aResponse()
                    .withStatus(403)
                    .withBody("{\"error\":{\"code\":\"forbidden\",\"message\":\"no\"}}")));

    HotApiException error = assertThrows(HotApiException.class, () -> client().env().get());

    assertEquals(HotApiException.class, error.getClass());
    assertNull(error.retryAfter());
  }

  @Test
  void asyncOrgViewsDelegate() throws Exception {
    AsyncHotClient async = AsyncHotClient.wrap(client());
    server.resetRequests();

    assertEquals(Map.of("ok", true), async.orgs().get("o1").get());
    assertEquals(Map.of("ok", true), async.orgs().env("o1", "e1").getProject("p").get());
    async.apiKeys().revoke("k1").get();

    List<String> urls =
        server.findAll(anyRequestedFor(anyUrl())).stream().map(LoggedRequest::getUrl).toList();
    assertEquals(List.of("/v1/orgs/o1", "/v1/orgs/o1/envs/e1/projects/p", "/v1/api-keys/k1"), urls);
    assertInstanceOf(AsyncHotClient.Orgs.class, async.orgs());
  }
}
