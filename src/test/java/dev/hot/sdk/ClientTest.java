package dev.hot.sdk;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.absent;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.patch;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ClientTest {

  private WireMockServer server;

  @BeforeEach
  void start() {
    // HTTP/1.1 only, as the Hot API serves plain HTTP: the JDK 17 client
    // drops reused h2c connections mid-retry.
    server = new WireMockServer(options().dynamicPort().http2PlainDisabled(true));
    server.start();
  }

  @AfterEach
  void stop() {
    server.stop();
  }

  private HotClient client() {
    return HotClient.builder("test-token").baseUrl(server.baseUrl()).build();
  }

  static ResponseDefinitionBuilder sse(String... events) {
    StringBuilder body = new StringBuilder();
    for (String event : events) {
      body.append("data: ").append(event).append("\n\n");
    }
    return aResponse()
        .withStatus(200)
        .withHeader("Content-Type", "text/event-stream")
        .withBody(body.toString());
  }

  private static String envelope(String dataJson) {
    return "{\"data\":" + dataJson + ",\"meta\":{\"request_id\":\"req_123\"}}";
  }

  @Test
  void publishEventSendsBearerTokenAndReturnsData() {
    server.stubFor(
        post(urlPathEqualTo("/v1/events"))
            .withHeader("Authorization", equalTo("Bearer test-token"))
            .withRequestBody(
                equalToJson(
                    "{\"event_type\":\"team-agent:ask\",\"event_data\":{\"question\":\"what changed?\"}}"))
            .willReturn(
                aResponse()
                    .withHeader("Content-Type", "application/json")
                    .withBody(
                        envelope("{\"event_id\":\"evt_123\",\"stream_id\":\"str_123\"}"))));

    Map<String, Object> event =
        client()
            .events()
            .publish(
                Map.of(
                    "event_type", "team-agent:ask",
                    "event_data", Map.of("question", "what changed?")));

    assertEquals("evt_123", event.get("event_id"));
  }

  @Test
  void listMethodsReturnApiListEnvelope() {
    server.stubFor(
        get(urlPathEqualTo("/v1/runs"))
            .withQueryParam("status", equalTo("completed"))
            .willReturn(
                aResponse()
                    .withHeader("Content-Type", "application/json")
                    .withBody(
                        "{\"data\":[{\"id\":\"run_123\"}],\"meta\":{},"
                            + "\"pagination\":{\"page\":1,\"per_page\":20,\"total\":1,\"total_pages\":1}}")));

    Map<String, Object> response = client().runs().list(Map.of("status", "completed"));

    assertEquals(List.of(Map.of("id", "run_123")), response.get("data"));
    assertEquals(1, ((Map<?, ?>) response.get("pagination")).get("total"));
  }

  @Test
  void streamsParseSseEvents() {
    server.stubFor(
        post(urlPathEqualTo("/v1/streams/subscribe-with-event"))
            .willReturn(
                sse(
                    "{\"type\":\"stream:data\",\"data_type\":\"text\",\"payload\":\"hi\"}",
                    "{\"type\":\"run:stop\",\"run\":{\"result\":\"done\"}}")));

    List<Map<String, Object>> events = new ArrayList<>();
    try (StreamEvents stream =
        client()
            .streams()
            .subscribeWithEvent(
                Map.of("event_type", "team-agent:ask"),
                new SubscribeWithEventOptions().reconnect(false))) {
      stream.forEachRemaining(events::add);
    }

    assertEquals(
        List.of(
            Map.of("type", "stream:data", "data_type", "text", "payload", "hi"),
            Map.of("type", "run:stop", "run", Map.of("result", "done"))),
        events);
  }

  @Test
  void apiErrorIsStructured() {
    server.stubFor(
        get(urlPathEqualTo("/v1/events"))
            .willReturn(
                aResponse()
                    .withStatus(401)
                    .withHeader("Retry-After", "5")
                    .withHeader("Content-Type", "application/json")
                    .withBody(
                        "{\"error\":{\"code\":\"unauthorized\",\"message\":\"Bad token\","
                            + "\"request_id\":\"req_123\",\"retry_after\":5}}")));

    HotApiException error =
        assertThrows(HotApiException.class, () -> client().events().list());

    assertEquals(401, error.statusCode());
    assertEquals("unauthorized", error.code());
    assertEquals("req_123", error.requestId());
    assertEquals(5, error.retryAfter());
    assertEquals("Bad token", error.getMessage());
  }

  @Test
  void apiErrorFallsBackToRetryAfterHeader() {
    server.stubFor(
        get(urlPathEqualTo("/v1/events"))
            .willReturn(
                aResponse().withStatus(503).withHeader("Retry-After", "30").withBody("slow down")));

    HotApiException error =
        assertThrows(HotApiException.class, () -> client().events().list());

    assertEquals(503, error.statusCode());
    assertEquals(30, error.retryAfter());
    assertEquals("slow down", error.getMessage());
    assertNull(error.code());
  }

  @Test
  void retries429WithRetryAfterAndSendsUserAgent() {
    server.stubFor(
        get(urlPathEqualTo("/v1/runs/run_123"))
            .withHeader("User-Agent", equalTo(dev.hot.sdk.internal.Transport.USER_AGENT))
            .inScenario("retry")
            .whenScenarioStateIs(com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED)
            .willSetStateTo("retried")
            .willReturn(
                aResponse()
                    .withStatus(429)
                    .withHeader("Retry-After", "1")
                    .withHeader("Content-Type", "application/json")
                    .withBody("{\"error\":{\"code\":\"rate_limited\",\"message\":\"Slow down\"}}")));
    server.stubFor(
        get(urlPathEqualTo("/v1/runs/run_123"))
            .inScenario("retry")
            .whenScenarioStateIs("retried")
            .willReturn(
                aResponse()
                    .withHeader("Content-Type", "application/json")
                    .withBody(envelope("{\"id\":\"run_123\"}"))));

    Map<String, Object> run = client().runs().get("run_123");

    assertEquals("run_123", run.get("id"));
    assertEquals(2, server.getAllServeEvents().size());
  }

  @Test
  void doesNotRetry429WithoutRetryAfter() {
    server.stubFor(
        get(urlPathEqualTo("/v1/runs/run_123"))
            .willReturn(
                aResponse()
                    .withStatus(429)
                    .withHeader("Content-Type", "application/json")
                    .withBody("{\"error\":{\"code\":\"rate_limited\",\"message\":\"Slow down\"}}")));

    HotApiException error =
        assertThrows(HotApiException.class, () -> client().runs().get("run_123"));

    assertEquals(429, error.statusCode());
    assertEquals(1, server.getAllServeEvents().size());
  }

  @Test
  void waitForRunResultTimesOutOnSilentStream() {
    server.stubFor(
        get(urlPathEqualTo("/v1/streams/s1/subscribe"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "text/event-stream")
                    .withBody(": keepalive\n\n".repeat(50))
                    .withChunkedDribbleDelay(50, 20_000)));

    long started = System.nanoTime();
    assertThrows(
        HotTimeoutException.class,
        () ->
            client()
                .streams()
                .waitForRunResult(
                    "s1", "e1", new WaitOptions().timeout(java.time.Duration.ofMillis(750))));
    long elapsedMillis = (System.nanoTime() - started) / 1_000_000;
    assertTrue(elapsedMillis < 10_000, "took " + elapsedMillis + "ms");
  }

  @Test
  void callHotUsesNestedRunFailureResult() {
    server.stubFor(
        post(urlPathEqualTo("/v1/streams/subscribe-with-event"))
            .willReturn(
                sse(
                    "{\"type\":\"event:published\",\"event_id\":\"evt_123\",\"stream_id\":\"str_123\"}",
                    "{\"type\":\"run:start\",\"run\":{\"run_id\":\"run_123\",\"event_id\":\"evt_123\"}}",
                    "{\"type\":\"run:fail\",\"run\":{\"run_id\":\"run_123\",\"event_id\":\"evt_123\","
                        + "\"status\":\"failed\",\"result\":{\"$err\":{\"message\":\"boom\"}}}}")));

    HotRunException error =
        assertThrows(HotRunException.class, () -> client().events().callHot("demo/fn", null));

    assertEquals("boom", error.getMessage());
  }

  @Test
  void callHotSurfacesHotRunFailureShape() {
    server.stubFor(
        post(urlPathEqualTo("/v1/streams/subscribe-with-event"))
            .willReturn(
                sse(
                    "{\"type\":\"event:published\",\"event_id\":\"evt_123\",\"stream_id\":\"str_123\"}",
                    "{\"type\":\"run:start\",\"run\":{\"run_id\":\"run_123\",\"event_id\":\"evt_123\"}}",
                    "{\"type\":\"run:fail\",\"run\":{\"run_id\":\"run_123\",\"event_id\":\"evt_123\","
                        + "\"status\":\"failed\",\"result\":{\"$type\":\"::hot::run/Failure\","
                        + "\"$origin\":{\"function\":\"::hot::exec/fail\"},"
                        + "\"$val\":{\"msg\":\"too hot\",\"err\":\"too hot\"}}}}")));

    HotRunException error =
        assertThrows(HotRunException.class, () -> client().events().callHot("demo/fn", null));

    assertEquals("too hot", error.getMessage());
  }

  @Test
  void callHotUnwrapsOkResultAndStreamsChunks() {
    server.stubFor(
        post(urlPathEqualTo("/v1/streams/subscribe-with-event"))
            .withRequestBody(
                equalToJson(
                    "{\"event_type\":\"hot:call\",\"event_data\":{\"fn\":\"demo/fn\",\"args\":[1,2]}}"))
            .willReturn(
                sse(
                    "{\"type\":\"event:published\",\"event_id\":\"evt_1\",\"stream_id\":\"str_1\"}",
                    "{\"type\":\"run:start\",\"run\":{\"run_id\":\"r1\",\"event_id\":\"evt_1\"}}",
                    "{\"type\":\"stream:data\",\"run_id\":\"r1\",\"payload\":{\"text\":\"partial\"}}",
                    "{\"type\":\"run:stop\",\"run\":{\"run_id\":\"r1\",\"event_id\":\"evt_1\","
                        + "\"result\":{\"$ok\":42}}}")));

    List<String> chunks = new ArrayList<>();
    Object result =
        client()
            .events()
            .callHot("demo/fn", List.of(1, 2), new CallOptions().onChunk(chunks::add));

    assertEquals(42, result);
    assertEquals(List.of("partial"), chunks);
  }

  @Test
  void requestEscapeHatchHandlesEmptyBody() {
    server.stubFor(post(urlPathEqualTo("/v1/new-endpoint")).willReturn(aResponse().withStatus(204)));

    Map<String, Object> response = client().request("POST", "/new-endpoint", null, null);

    assertNull(response.get("data"));
    assertTrue(response.containsKey("meta"));
  }

  @Test
  void pathSegmentsAreEscaped() {
    server.stubFor(
        get(urlEqualTo("/v1/projects/a%2Fb%20c"))
            .willReturn(
                aResponse()
                    .withHeader("Content-Type", "application/json")
                    .withBody(envelope("{}"))));

    Map<String, Object> project = client().projects().get("a/b c");

    assertEquals(Map.of(), project);
  }

  @Test
  void asyncClientWrapsSyncResources() throws Exception {
    server.stubFor(
        post(urlPathEqualTo("/v1/events"))
            .willReturn(
                aResponse()
                    .withHeader("Content-Type", "application/json")
                    .withBody(envelope("{\"event_id\":\"evt_9\",\"stream_id\":\"str_9\"}"))));

    AsyncHotClient async = AsyncHotClient.wrap(client());
    Map<String, Object> event =
        async.events().publish(Map.of("event_type", "x", "event_data", Map.of())).get();

    assertEquals("evt_9", event.get("event_id"));
  }

  @Test
  void builderRequiresToken() {
    assertThrows(IllegalArgumentException.class, () -> HotClient.builder("").build());
  }

  @Test
  void statusCallsRootStatusWithoutAuthorizationHeader() {
    server.stubFor(
        get(urlEqualTo("/status"))
            .withHeader("Authorization", absent())
            .withHeader("Accept", equalTo("application/json"))
            .withHeader("User-Agent", equalTo(dev.hot.sdk.internal.Transport.USER_AGENT))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody("{\"status\":\"ok\",\"version\":\"3.0.14\"}")));

    // Also verifies /v1 suffix on baseUrl is stripped before calling /status.
    HotClient clientWithV1 =
        HotClient.builder("test-token").baseUrl(server.baseUrl() + "/v1/").build();
    Map<String, Object> status = clientWithV1.status();

    assertEquals("ok", status.get("status"));
    assertEquals("3.0.14", status.get("version"));
    server.verify(1, getRequestedFor(urlEqualTo("/status")));
  }

  @Test
  void non2xx3xxResponsesThrowHotApiExceptionAcrossJsonRawAndStream() {
    server.stubFor(
        get(urlPathEqualTo("/v1/projects/p1"))
            .willReturn(
                aResponse().withStatus(302).withHeader("Location", "https://evil.example/")));
    server.stubFor(
        get(urlPathEqualTo("/v1/files/f1/download"))
            .willReturn(
                aResponse().withStatus(301).withHeader("Location", "https://evil.example/")));
    server.stubFor(
        get(urlPathEqualTo("/v1/runs/r1/subscribe"))
            .willReturn(
                aResponse().withStatus(307).withHeader("Location", "https://evil.example/")));

    HotApiException jsonErr =
        assertThrows(HotApiException.class, () -> client().projects().get("p1"));
    assertEquals(302, jsonErr.statusCode());

    HotApiException rawErr =
        assertThrows(HotApiException.class, () -> client().files().download("f1"));
    assertEquals(301, rawErr.statusCode());

    HotApiException streamErr =
        assertThrows(HotApiException.class, () -> client().runs().subscribe("r1"));
    assertEquals(307, streamErr.statusCode());
  }

  @Test
  void publishEventIdRetries503EventPublishInProgressAndSurfaces409Conflict() {
    // 1. 503 event_publish_in_progress with retry_after -> retries and succeeds.
    server.stubFor(
        post(urlPathEqualTo("/v1/events"))
            .withRequestBody(
                equalToJson(
                    "{\"event_id\":\"evt_idem_1\",\"event_type\":\"order:placed\",\"event_data\":{}}"))
            .inScenario("publish-in-progress")
            .whenScenarioStateIs(Scenario.STARTED)
            .willSetStateTo("retry1")
            .willReturn(
                aResponse()
                    .withStatus(503)
                    .withHeader("Content-Type", "application/json")
                    .withBody(
                        "{\"error\":{\"code\":\"event_publish_in_progress\","
                            + "\"message\":\"in progress\",\"retry_after\":1}}")));
    server.stubFor(
        post(urlPathEqualTo("/v1/events"))
            .inScenario("publish-in-progress")
            .whenScenarioStateIs("retry1")
            .willSetStateTo("done")
            .willReturn(
                aResponse()
                    .withStatus(503)
                    .withHeader("Content-Type", "application/json")
                    .withBody(
                        "{\"error\":{\"code\":\"event_publish_in_progress\","
                            + "\"message\":\"still in progress\"}}")));
    server.stubFor(
        post(urlPathEqualTo("/v1/events"))
            .inScenario("publish-in-progress")
            .whenScenarioStateIs("done")
            .willReturn(
                aResponse()
                    .withHeader("Content-Type", "application/json")
                    .withBody(envelope("{\"event_id\":\"evt_idem_1\",\"stream_id\":\"s1\"}"))));

    Map<String, Object> published =
        client()
            .events()
            .publish(
                Map.of(
                    "event_id", "evt_idem_1",
                    "event_type", "order:placed",
                    "event_data", Map.of()));
    assertEquals("evt_idem_1", published.get("event_id"));
    assertEquals(3, server.getAllServeEvents().size());

    // 2. 409 event_id_conflict is NOT retried.
    server.resetAll();
    server.stubFor(
        post(urlPathEqualTo("/v1/events"))
            .willReturn(
                aResponse()
                    .withStatus(409)
                    .withHeader("Content-Type", "application/json")
                    .withBody(
                        "{\"error\":{\"code\":\"event_id_conflict\","
                            + "\"message\":\"payload mismatch\"}}")));

    HotApiException conflict =
        assertThrows(
            HotApiException.class,
            () ->
                client()
                    .events()
                    .publish(
                        Map.of(
                            "event_id", "evt_idem_1",
                            "event_type", "order:other",
                            "event_data", Map.of())));
    assertEquals(409, conflict.statusCode());
    assertEquals("event_id_conflict", conflict.code());
    assertEquals(1, server.getAllServeEvents().size());

    // 3. Plain 503 (different error code) on POST is NOT retried.
    server.resetAll();
    server.stubFor(
        post(urlPathEqualTo("/v1/events"))
            .willReturn(
                aResponse()
                    .withStatus(503)
                    .withHeader("Content-Type", "application/json")
                    .withBody("{\"error\":{\"code\":\"unavailable\",\"message\":\"down\"}}")));

    HotApiException unavailable =
        assertThrows(
            HotApiException.class,
            () -> client().events().publish(Map.of("event_type", "x", "event_data", Map.of())));
    assertEquals(503, unavailable.statusCode());
    assertEquals(1, server.getAllServeEvents().size());
  }

  @Test
  void idempotentMethodsRetry502And504WhilePatchDoesNot() {
    // Lowercase "get" is normalized to GET and treated as idempotent (retried on 502).
    server.stubFor(
        get(urlPathEqualTo("/v1/projects"))
            .inScenario("get-502")
            .whenScenarioStateIs(Scenario.STARTED)
            .willSetStateTo("ok")
            .willReturn(aResponse().withStatus(502).withBody("bad gateway")));
    server.stubFor(
        get(urlPathEqualTo("/v1/projects"))
            .inScenario("get-502")
            .whenScenarioStateIs("ok")
            .willReturn(aResponse().withBody(envelope("[]"))));

    // PUT retries on 504.
    server.stubFor(
        put(urlPathEqualTo("/v1/projects/p1/context/k1"))
            .inScenario("put-504")
            .whenScenarioStateIs(Scenario.STARTED)
            .willSetStateTo("ok")
            .willReturn(aResponse().withStatus(504).withBody("gateway timeout")));
    server.stubFor(
        put(urlPathEqualTo("/v1/projects/p1/context/k1"))
            .inScenario("put-504")
            .whenScenarioStateIs("ok")
            .willReturn(aResponse().withBody(envelope("{\"key\":\"k1\"}"))));

    // PATCH does NOT retry on 503.
    server.stubFor(
        patch(urlPathEqualTo("/v1/projects/p1"))
            .willReturn(aResponse().withStatus(503).withBody("unavailable")));

    // Missing leading slash in request() is also normalized.
    Map<String, Object> projects = client().request("get", "projects", null, null);
    assertEquals(List.of(), projects.get("data"));
    assertEquals("k1", client().context().update("p1", "k1", Map.of("value", "v")).get("key"));

    HotApiException patchError =
        assertThrows(
            HotApiException.class,
            () -> client().projects().update("p1", Map.of("name", "renamed")));
    assertEquals(503, patchError.statusCode());
    assertEquals(
        1,
        server
            .findAll(
                com.github.tomakehurst.wiremock.client.WireMock.patchRequestedFor(
                    urlPathEqualTo("/v1/projects/p1")))
            .size());
  }

  @Test
  void retrySleepIsBoundedByTotalClientTimeout() {
    server.stubFor(
        get(urlPathEqualTo("/v1/runs/r1"))
            .willReturn(
                aResponse()
                    .withStatus(429)
                    .withHeader("Retry-After", "10")
                    .withBody("{\"error\":{\"code\":\"rate_limited\",\"message\":\"slow\"}}")));

    HotClient fastTimeout =
        HotClient.builder("test-token")
            .baseUrl(server.baseUrl())
            .timeout(Duration.ofMillis(300))
            .build();
    long started = System.nanoTime();
    HotApiException error = assertThrows(HotApiException.class, () -> fastTimeout.runs().get("r1"));
    long elapsedMs = (System.nanoTime() - started) / 1_000_000;

    assertEquals(429, error.statusCode());
    assertTrue(elapsedMs < 2_000, "should not sleep 10s past 300ms timeout, took " + elapsedMs);
    assertEquals(1, server.getAllServeEvents().size());
  }

  @Test
  void requestRawUsesSetHeaderToReplaceDefaultHeaders() throws Exception {
    server.stubFor(
        get(urlPathEqualTo("/v1/files/f1/download"))
            .willReturn(aResponse().withStatus(200).withBody("ok")));

    try (var body =
        client()
            .requestRaw(
                "GET",
                "files/f1/download",
                null,
                Map.of("User-Agent", "custom-agent/1.0"),
                BodyPublishers.noBody())
            .body()) {
      assertEquals("ok", new String(body.readAllBytes(), StandardCharsets.UTF_8));
    }

    LoggedRequest req =
        server.findAll(getRequestedFor(urlPathEqualTo("/v1/files/f1/download"))).get(0);
    assertEquals(List.of("custom-agent/1.0"), req.getHeaders().getHeader("User-Agent").values());
  }

  @Test
  void sseOverHttpEnforcesUtf8StripsBomAndParsesCrLf() {
    byte[] bom = new byte[] {(byte) 0xEF, (byte) 0xBB, (byte) 0xBF};
    byte[] payload =
        ("data: {\"type\":\"stream:data\",\"text\":\"caf\u00E9\"}\r\n\r\n"
                + "data: {\r\n"
                + "data: \"type\":\"run:stop\",\r\n"
                + "data: \"run\":{\"result\":\"ok\"}\r\n"
                + "data: }\r\n\r\n")
            .getBytes(StandardCharsets.UTF_8);
    byte[] body = new byte[bom.length + payload.length];
    System.arraycopy(bom, 0, body, 0, bom.length);
    System.arraycopy(payload, 0, body, bom.length, payload.length);

    server.stubFor(
        get(urlPathEqualTo("/v1/streams/s1/subscribe"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    // Deliberately non-UTF-8 charset to prove WHATWG UTF-8 enforcement.
                    .withHeader("Content-Type", "text/event-stream; charset=iso-8859-1")
                    .withBody(body)));

    List<Map<String, Object>> events = new ArrayList<>();
    try (StreamEvents stream = client().streams().subscribe("s1")) {
      stream.forEachRemaining(events::add);
    }

    assertEquals(2, events.size());
    assertEquals("caf\u00E9", events.get(0).get("text"));
    assertEquals("ok", StreamEvents.runOf(events.get(1)).get("result"));
  }

  @Test
  void nullOptionsDefaultCleanlyWithoutNpe() {
    server.stubFor(
        post(urlPathEqualTo("/v1/streams/subscribe-with-event"))
            .willReturn(
                sse(
                    "{\"type\":\"event:published\",\"event_id\":\"e1\",\"stream_id\":\"s1\"}",
                    "{\"type\":\"run:stop\",\"run\":{\"run_id\":\"r1\",\"event_id\":\"e1\","
                        + "\"status\":\"succeeded\",\"result\":{\"$ok\":\"hi\"}}}")));
    server.stubFor(
        get(urlPathEqualTo("/v1/streams/s1/subscribe"))
            .willReturn(
                sse(
                    "{\"type\":\"run:stop\",\"run\":{\"run_id\":\"r1\",\"event_id\":\"e1\","
                        + "\"status\":\"succeeded\",\"result\":\"done\"}}")));
    server.stubFor(
        get(urlPathEqualTo("/v1/runs/r1/subscribe"))
            .willReturn(
                sse("{\"type\":\"run:update\",\"run\":{\"run_id\":\"r1\",\"status\":\"succeeded\"}}")));
    server.stubFor(
        get(urlPathEqualTo("/v1/tasks/t1/subscribe"))
            .willReturn(
                sse(
                    "{\"type\":\"task:update\",\"task\":{\"task_id\":\"t1\",\"status\":\"completed\"}}")));

    HotClient c = client();
    assertEquals("hi", c.events().callHot("demo/fn", List.of(), null));
    assertEquals("done", c.streams().waitForRunResult("s1", "e1", null).get("result"));
    List<Map<String, Object>> subEvents = new ArrayList<>();
    try (StreamEvents stream = c.streams().subscribeWithEvent(Map.of("event_type", "x"), null)) {
      stream.forEachRemaining(subEvents::add);
    }
    assertEquals(2, subEvents.size());
    assertEquals("succeeded", c.runs().waitFor("r1", null).get("status"));
    assertEquals("completed", c.tasks().waitFor("t1", null).get("status"));
  }

  @Test
  void waiterStopsImmediatelyWhenThreadIsInterrupted() {
    server.stubFor(
        get(urlPathEqualTo("/v1/runs/r1/subscribe"))
            .willReturn(aResponse().withStatus(503).withBody("unavailable")));

    Thread.currentThread().interrupt();
    try {
      HotTransportException error =
          assertThrows(HotTransportException.class, () -> client().runs().waitFor("r1"));
      assertTrue(error.getCause() instanceof InterruptedException);
      assertTrue(Thread.currentThread().isInterrupted());
      assertEquals(0, server.getAllServeEvents().size());
    } finally {
      // Clear interrupt flag so subsequent tests on this thread are unaffected.
      Thread.interrupted();
    }
  }

  @Test
  void oversizedErrorBodyIsCappedAndTruncated() {
    String hugeHtml = "<html>" + "x".repeat(1_500_000) + "</html>";
    server.stubFor(
        get(urlPathEqualTo("/v1/projects/p1"))
            .willReturn(
                aResponse()
                    .withStatus(500)
                    .withHeader("Content-Type", "text/html")
                    .withBody(hugeHtml)));

    HotApiException error =
        assertThrows(HotApiException.class, () -> client().projects().get("p1"));
    assertEquals(500, error.statusCode());
    assertTrue(error.getMessage().length() <= 4100);
    assertFalse(error.getMessage().isEmpty());
  }
}
