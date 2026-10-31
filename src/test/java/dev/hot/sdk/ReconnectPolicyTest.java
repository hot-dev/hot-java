package dev.hot.sdk;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.exactly;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.client.ScenarioMappingBuilder;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The shared reconnect policy: permanent errors stop at once, transient ones back off (honouring
 * Retry-After), and only consecutive unproductive connections count against maxAttempts.
 */
class ReconnectPolicyTest {

  private static final String PUBLISHED =
      "{\"type\":\"event:published\",\"stream_id\":\"s1\",\"event_id\":\"e1\"}";
  private static final String START =
      "{\"type\":\"run:start\",\"run\":{\"run_id\":\"r1\",\"event_id\":\"e1\",\"status\":\"running\"}}";
  private static final String STOP =
      "{\"type\":\"run:stop\",\"run\":{\"run_id\":\"r1\",\"event_id\":\"e1\","
          + "\"status\":\"succeeded\",\"result\":\"done\"}}";
  private static final String COMPLETE = "{\"type\":\"stream:complete\",\"stream_id\":\"s1\"}";
  private static final String RUNNING =
      "{\"type\":\"run:update\",\"run\":{\"run_id\":\"r1\",\"status\":\"running\"}}";
  private static final String SUCCEEDED =
      "{\"type\":\"run:update\",\"run\":{\"run_id\":\"r1\",\"status\":\"succeeded\"}}";

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

  /** Stubs {@code responses} in order; the last one repeats. */
  private void sequence(
      Function<String, MappingBuilder> request, String path, ResponseDefinitionBuilder... responses) {
    for (int i = 0; i < responses.length; i++) {
      ScenarioMappingBuilder mapping =
          request
              .apply(path)
              .inScenario(path)
              .whenScenarioStateIs(i == 0 ? Scenario.STARTED : "step" + i);
      if (i + 1 < responses.length) {
        mapping = mapping.willSetStateTo("step" + (i + 1));
      }
      server.stubFor(mapping.willReturn(responses[i]));
    }
  }

  private static Function<String, MappingBuilder> getPath() {
    return path -> get(urlPathEqualTo(path));
  }

  private static ResponseDefinitionBuilder status(int status) {
    return aResponse()
        .withStatus(status)
        .withHeader("Content-Type", "application/json")
        .withBody("{\"error\":{\"code\":\"x\",\"message\":\"status " + status + "\"}}");
  }

  private int requests(String path) {
    return server.findAll(getRequestedFor(urlPathEqualTo(path))).size();
  }

  @ParameterizedTest
  @ValueSource(ints = {400, 401, 403, 404, 410, 422})
  void runWaitStopsOnPermanentErrors(int code) {
    sequence(getPath(), "/v1/runs/r1/subscribe", status(code));

    HotApiException error =
        assertThrows(HotApiException.class, () -> client().runs().waitFor("r1"));

    assertEquals(code, error.statusCode());
    assertEquals(1, requests("/v1/runs/r1/subscribe"));
  }

  @Test
  void permanentErrorsStopEveryWaiter() {
    sequence(getPath(), "/v1/tasks/t1/subscribe", status(403));
    sequence(getPath(), "/v1/streams/s1/subscribe", status(404));
    HotClient client = client();

    assertThrows(HotApiException.class, () -> client.tasks().waitFor("t1"));
    assertThrows(HotApiException.class, () -> client.streams().waitForRunResult("s1", "e1"));

    assertEquals(1, requests("/v1/tasks/t1/subscribe"));
    assertEquals(1, requests("/v1/streams/s1/subscribe"));
  }

  @Test
  void transientErrorsBackOffBeforeReconnecting() {
    sequence(
        getPath(), "/v1/runs/r1/subscribe", status(503), status(502), ClientTest.sse(SUCCEEDED));

    long started = System.nanoTime();
    Map<String, Object> run = client().runs().waitFor("r1");
    long elapsedMillis = (System.nanoTime() - started) / 1_000_000;

    assertEquals("succeeded", run.get("status"));
    assertEquals(3, requests("/v1/runs/r1/subscribe"));
    // Two failures: at least 250 ms and 500 ms of jittered backoff.
    assertTrue(elapsedMillis >= 750, "reconnected after " + elapsedMillis + " ms");
  }

  @Test
  void retryAfterIsHonoured() {
    sequence(
        getPath(),
        "/v1/runs/r1/subscribe",
        status(429).withHeader("Retry-After", "2"),
        ClientTest.sse(SUCCEEDED));

    long started = System.nanoTime();
    client().runs().waitFor("r1");
    long elapsedMillis = (System.nanoTime() - started) / 1_000_000;

    assertTrue(elapsedMillis >= 2000, "reconnected after " + elapsedMillis + " ms");
  }

  @Test
  void backoffThatWouldPassTheDeadlineTimesOut() {
    sequence(
        getPath(),
        "/v1/runs/r1/subscribe",
        status(503).withHeader("Retry-After", "30"),
        ClientTest.sse(SUCCEEDED));

    long started = System.nanoTime();
    HotTimeoutException error =
        assertThrows(
            HotTimeoutException.class,
            () -> client().runs().waitFor("r1", new RunWaitOptions().timeout(Duration.ofSeconds(2))));
    long elapsedMillis = (System.nanoTime() - started) / 1_000_000;

    assertTrue(elapsedMillis < 1500, "timed out after " + elapsedMillis + " ms");
    assertTrue(error.getCause() instanceof HotApiException, String.valueOf(error.getCause()));
    assertEquals(1, requests("/v1/runs/r1/subscribe"));
  }

  @Test
  void consecutiveFailuresExhaustTheBudget() {
    sequence(getPath(), "/v1/runs/r1/subscribe", status(503));

    HotApiException error =
        assertThrows(
            HotApiException.class,
            () -> client().runs().waitFor("r1", new RunWaitOptions().maxAttempts(1)));

    assertEquals(503, error.statusCode());
    assertEquals(2, requests("/v1/runs/r1/subscribe"));
  }

  @Test
  void rolloversDoNotCountAgainstTheBudget() {
    // Six 5-minute windows, each ending with stream:complete, with a budget of one.
    ResponseDefinitionBuilder rollover = ClientTest.sse(RUNNING, COMPLETE);
    sequence(
        getPath(),
        "/v1/runs/r1/subscribe",
        rollover,
        rollover,
        rollover,
        rollover,
        rollover,
        rollover,
        ClientTest.sse(SUCCEEDED));

    long started = System.nanoTime();
    Map<String, Object> run = client().runs().waitFor("r1", new RunWaitOptions().maxAttempts(1));
    long elapsedMillis = (System.nanoTime() - started) / 1_000_000;

    assertEquals("succeeded", run.get("status"));
    assertEquals(7, requests("/v1/runs/r1/subscribe"));
    assertTrue(elapsedMillis < 2000, "rollovers reconnect without backoff: " + elapsedMillis);
  }

  @Test
  void runResultWaitSurvivesMoreRolloversThanAttempts() {
    ResponseDefinitionBuilder rollover = ClientTest.sse(START, COMPLETE);
    sequence(
        getPath(),
        "/v1/streams/s1/subscribe",
        rollover,
        rollover,
        rollover,
        rollover,
        ClientTest.sse(START, STOP));

    Map<String, Object> run =
        client().streams().waitForRunResult("s1", "e1", new WaitOptions().maxAttempts(1));

    assertEquals("done", run.get("result"));
    assertEquals(5, requests("/v1/streams/s1/subscribe"));
  }

  @Test
  void callHotSurvivesMoreRolloversThanAttempts() {
    server.stubFor(
        post(urlPathEqualTo("/v1/streams/subscribe-with-event"))
            .willReturn(ClientTest.sse(PUBLISHED, START, COMPLETE)));
    ResponseDefinitionBuilder rollover = ClientTest.sse(START, COMPLETE);
    sequence(
        getPath(),
        "/v1/streams/s1/subscribe",
        rollover,
        rollover,
        rollover,
        ClientTest.sse(START, STOP));

    Object result =
        client().events().callHot("demo/fn", List.of(), new CallOptions().maxAttempts(1));

    assertEquals("done", result);
    assertEquals(4, requests("/v1/streams/s1/subscribe"));
  }

  @Test
  void subscribeWithEventSurvivesMoreRolloversThanAttempts() {
    server.stubFor(
        post(urlPathEqualTo("/v1/streams/subscribe-with-event"))
            .willReturn(ClientTest.sse(PUBLISHED, START, COMPLETE)));
    ResponseDefinitionBuilder rollover = ClientTest.sse(START, COMPLETE);
    sequence(
        getPath(),
        "/v1/streams/s1/subscribe",
        rollover,
        rollover,
        rollover,
        rollover,
        rollover,
        rollover,
        ClientTest.sse(START, STOP));

    List<String> types = new ArrayList<>();
    try (StreamEvents events =
        client()
            .streams()
            .subscribeWithEvent(
                Map.of("event_type", "x", "event_data", Map.of()),
                new SubscribeWithEventOptions().maxAttempts(1))) {
      events.forEachRemaining(event -> types.add(StreamEvents.typeOf(event)));
    }

    assertEquals("run:stop", types.get(types.size() - 1));
    assertEquals(7, requests("/v1/streams/s1/subscribe"));
  }

  @Test
  void subscribeWithEventRetriesAFailedResubscribe() {
    server.stubFor(
        post(urlPathEqualTo("/v1/streams/subscribe-with-event"))
            .willReturn(ClientTest.sse(PUBLISHED, START)));
    sequence(
        getPath(),
        "/v1/streams/s1/subscribe",
        status(503),
        status(429).withHeader("Retry-After", "1"),
        ClientTest.sse(STOP));

    List<String> types = new ArrayList<>();
    try (StreamEvents events =
        client().streams().subscribeWithEvent(Map.of("event_type", "x", "event_data", Map.of()))) {
      events.forEachRemaining(event -> types.add(StreamEvents.typeOf(event)));
    }

    assertEquals(List.of("event:published", "run:start", "run:stop"), types);
    assertEquals(3, requests("/v1/streams/s1/subscribe"));
  }

  @Test
  void subscribeWithEventStopsOnPermanentResubscribeError() {
    server.stubFor(
        post(urlPathEqualTo("/v1/streams/subscribe-with-event"))
            .willReturn(ClientTest.sse(PUBLISHED, START)));
    sequence(getPath(), "/v1/streams/s1/subscribe", status(403));

    List<String> types = new ArrayList<>();
    HotApiException error =
        assertThrows(
            HotApiException.class,
            () -> {
              try (StreamEvents events =
                  client()
                      .streams()
                      .subscribeWithEvent(Map.of("event_type", "x", "event_data", Map.of()))) {
                events.forEachRemaining(event -> types.add(StreamEvents.typeOf(event)));
              }
            });

    assertEquals(403, error.statusCode());
    assertEquals(List.of("event:published", "run:start"), types);
    server.verify(exactly(1), getRequestedFor(urlPathEqualTo("/v1/streams/s1/subscribe")));
  }

  @Test
  void unproductiveConnectionsCountAgainstTheBudget() {
    server.stubFor(
        post(urlPathEqualTo("/v1/streams/subscribe-with-event"))
            .willReturn(ClientTest.sse(PUBLISHED, START)));
    // Replays of an already-seen run:start are not progress.
    sequence(getPath(), "/v1/streams/s1/subscribe", ClientTest.sse(START));

    HotStreamException error =
        assertThrows(
            HotStreamException.class,
            () -> {
              try (StreamEvents events =
                  client()
                      .streams()
                      .subscribeWithEvent(
                          Map.of("event_type", "x", "event_data", Map.of()),
                          new SubscribeWithEventOptions().maxAttempts(1))) {
                events.forEachRemaining(event -> {});
              }
            });

    assertTrue(error.getMessage().contains("ended before"), error.getMessage());
    assertEquals(2, requests("/v1/streams/s1/subscribe"));
  }
}
