package dev.hot.sdk;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
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
    server = new WireMockServer(options().dynamicPort());
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
}
