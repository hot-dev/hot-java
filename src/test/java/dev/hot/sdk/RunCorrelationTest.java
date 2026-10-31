package dev.hot.sdk;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.WireMockServer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Runs are correlated to the published event in either event order, and through retries. The
 * event sequences match hot main d5dde71: chunks and run:stop before run:start, and a retry run
 * with a new event_id that links back by origin_run_id.
 */
class RunCorrelationTest {

  private static final String PUBLISHED =
      "{\"type\":\"event:published\",\"stream_id\":\"s1\",\"event_id\":\"e1\"}";

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

  private static String run(String type, String runId, String eventId, String status, String extra) {
    return "{\"type\":\"" + type + "\",\"run\":{\"run_id\":\"" + runId + "\",\"event_id\":\""
        + eventId + "\",\"status\":\"" + status + "\"" + extra + "}}";
  }

  private static String chunk(String runId, String text) {
    return "{\"type\":\"stream:data\",\"run_id\":\"" + runId
        + "\",\"data_type\":\"chunk\",\"payload\":{\"text\":\"" + text + "\"}}";
  }

  private void callStream(String... events) {
    String[] all = new String[events.length + 1];
    all[0] = PUBLISHED;
    System.arraycopy(events, 0, all, 1, events.length);
    server.stubFor(post(urlPathEqualTo("/v1/streams/subscribe-with-event"))
        .willReturn(ClientTest.sse(all)));
  }

  @Test
  void chunksBeforeRunStartAreDelivered() {
    // hot main today: data and run:stop arrive before run:start.
    callStream(
        chunk("r1", "a"),
        chunk("r1", "b"),
        chunk("r1", "c"),
        run("run:stop", "r1", "e1", "succeeded", ",\"result\":\"done\""),
        run("run:start", "r1", "e1", "succeeded", ""));

    List<String> chunks = new ArrayList<>();
    Object result =
        client().events().callHot("demo/fn", List.of(), new CallOptions().onChunk(chunks::add));

    assertEquals("done", result);
    assertEquals(List.of("a", "b", "c"), chunks);
  }

  @Test
  void chunksAfterRunStartAreDelivered() {
    // The documented order, once the server publishes run:start (hot PR 4).
    callStream(
        run("run:start", "r1", "e1", "running", ""),
        chunk("r1", "a"),
        chunk("r1", "b"),
        run("run:stop", "r1", "e1", "succeeded", ",\"result\":\"done\""));

    List<String> chunks = new ArrayList<>();
    client().events().callHot("demo/fn", List.of(), new CallOptions().onChunk(chunks::add));

    assertEquals(List.of("a", "b"), chunks);
  }

  @Test
  void chunksOfOtherRunsAreDropped() {
    callStream(
        chunk("other", "x"),
        chunk("r1", "a"),
        run("run:stop", "other", "e2", "succeeded", ",\"result\":\"other\""),
        run("run:stop", "r1", "e1", "succeeded", ",\"result\":\"done\""));

    List<String> chunks = new ArrayList<>();
    Object result =
        client().events().callHot("demo/fn", List.of(), new CallOptions().onChunk(chunks::add));

    assertEquals("done", result);
    assertEquals(List.of("a"), chunks);
  }

  private static final String[] RETRY_SEQUENCE = {
    run("run:fail", "rA", "e1", "pending_retry", ",\"result\":\"first attempt\""),
    run("run:stop", "rB", "e9", "succeeded", ",\"origin_run_id\":\"rA\",\"result\":\"recovered\""),
    run("run:start", "rB", "e9", "succeeded", ",\"origin_run_id\":\"rA\""),
    run("run:start", "rA", "e1", "failed", ""),
  };

  @Test
  void waitForRunResultFollowsTheRetryRun() {
    server.stubFor(get(urlPathEqualTo("/v1/streams/s1/subscribe"))
        .willReturn(ClientTest.sse(RETRY_SEQUENCE)));

    Map<String, Object> run = client().streams().waitForRunResult("s1", "e1");

    assertEquals("rB", run.get("run_id"));
    assertEquals("recovered", run.get("result"));
  }

  @Test
  void callHotFollowsTheRetryRun() {
    callStream(RETRY_SEQUENCE);

    assertEquals("recovered", client().events().callHot("demo/fn", List.of()));
  }

  // The three ways hot 3.0.0 and d5dde71 show a retried first attempt A (sdk-retry-replay.md).

  @Test
  void replayCase1PendingRetryFailIsNotFinal() {
    server.stubFor(get(urlPathEqualTo("/v1/streams/s1/subscribe"))
        .willReturn(ClientTest.sse(
            run("run:fail", "rA", "e1", "pending_retry", ",\"retry_attempt\":1"),
            run("run:stop", "rB", "e9", "succeeded",
                ",\"origin_run_id\":\"rA\",\"retry_attempt\":1,\"result\":\"recovered\""))));

    assertEquals("rB", client().streams().waitForRunResult("s1", "e1").get("run_id"));
  }

  @Test
  void replayCase2PendingRetryStopIsNotASuccess() {
    // The poll maps every non-failed status, pending_retry included, to run:stop.
    server.stubFor(get(urlPathEqualTo("/v1/streams/s1/subscribe"))
        .willReturn(ClientTest.sse(
            run("run:stop", "rA", "e1", "pending_retry", ",\"retry_attempt\":1"),
            run("run:stop", "rB", "e9", "succeeded",
                ",\"origin_run_id\":\"rA\",\"retry_attempt\":1,\"result\":\"recovered\""))));

    Map<String, Object> run = client().streams().waitForRunResult("s1", "e1");

    assertEquals("rB", run.get("run_id"));
    assertEquals("recovered", run.get("result"));
  }

  @Test
  void replayCase3FailureWithRaisedAttemptWaitsForTheRetryAcrossReconnects() {
    // After the retry is queued and before its run exists, A replays as failed, attempt 1.
    sequence(
        "/v1/streams/s1/subscribe",
        ClientTest.sse(
            run("run:start", "rA", "e1", "failed", ",\"retry_attempt\":1"),
            run("run:fail", "rA", "e1", "failed",
                ",\"retry_attempt\":1,\"result\":\"first attempt\"")),
        ClientTest.sse(
            run("run:stop", "rB", "e9", "succeeded",
                ",\"origin_run_id\":\"rA\",\"retry_attempt\":1,\"result\":\"recovered\"")));

    assertEquals("rB", client().streams().waitForRunResult("s1", "e1").get("run_id"));
  }

  @Test
  void retryRunSeenBeforeItsOriginIsKeptAsAnOrphan() {
    // The poll replays newest runs first, so B can come before A ties.
    server.stubFor(get(urlPathEqualTo("/v1/streams/s1/subscribe"))
        .willReturn(ClientTest.sse(
            run("run:stop", "rB", "e9", "succeeded",
                ",\"origin_run_id\":\"rA\",\"retry_attempt\":1,\"result\":\"recovered\""),
            run("run:fail", "rA", "e1", "failed", ",\"retry_attempt\":1"))));

    assertEquals("rB", client().streams().waitForRunResult("s1", "e1").get("run_id"));
  }

  /** A response with {@code events} first, then keepalives dribbled out over 20 seconds. */
  private static com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder held(
      String... events) {
    StringBuilder body = new StringBuilder();
    for (String event : events) {
      body.append("data: ").append(event).append("\n\n");
    }
    body.append(": keepalive\n\n".repeat(400));
    return aResponse()
        .withStatus(200)
        .withHeader("Content-Type", "text/event-stream")
        .withBody(body.toString())
        .withChunkedDribbleDelay(200, 20_000);
  }

  @Test
  void heldFailureStandsWhenNoRetryAppearsWithinTheGrace() {
    server.stubFor(get(urlPathEqualTo("/v1/streams/s1/subscribe"))
        .willReturn(held(run("run:fail", "rA", "e1", "failed",
            ",\"retry_attempt\":1,\"result\":\"gave up\""))));

    long started = System.nanoTime();
    HotRunException error =
        assertThrows(
            HotRunException.class, () -> client().streams().waitForRunResult("s1", "e1"));
    long elapsedMillis = (System.nanoTime() - started) / 1_000_000;

    assertEquals("gave up", error.getMessage());
    assertTrue(elapsedMillis >= 4_500 && elapsedMillis < 10_000, "after " + elapsedMillis);
  }

  @Test
  void heldFailureStandsAtTheDeadlineIfThatComesFirst() {
    server.stubFor(get(urlPathEqualTo("/v1/streams/s1/subscribe"))
        .willReturn(held(run("run:fail", "rA", "e1", "failed",
            ",\"retry_attempt\":1,\"result\":\"gave up\""))));

    long started = System.nanoTime();
    HotRunException error =
        assertThrows(
            HotRunException.class,
            () ->
                client()
                    .streams()
                    .waitForRunResult(
                        "s1", "e1", new WaitOptions().timeout(java.time.Duration.ofSeconds(2))));
    long elapsedMillis = (System.nanoTime() - started) / 1_000_000;

    assertEquals("gave up", error.getMessage());
    assertTrue(elapsedMillis < 4_000, "after " + elapsedMillis);
  }

  @Test
  void failureWithoutARetryFailsAtOnce() {
    // retry_attempt 0 (a handler without retry): no grace.
    server.stubFor(get(urlPathEqualTo("/v1/streams/s1/subscribe"))
        .willReturn(held(run("run:fail", "rA", "e1", "failed",
            ",\"retry_attempt\":0,\"result\":\"boom\""))));

    long started = System.nanoTime();
    assertThrows(HotRunException.class, () -> client().streams().waitForRunResult("s1", "e1"));
    assertTrue((System.nanoTime() - started) / 1_000_000 < 3_000);
  }

  @Test
  void subscribeWithEventEndsAfterTheGraceWhenNoRetryAppears() {
    server.stubFor(post(urlPathEqualTo("/v1/streams/subscribe-with-event"))
        .willReturn(held(PUBLISHED, run("run:fail", "rA", "e1", "failed",
            ",\"retry_attempt\":1,\"result\":\"gave up\""))));

    List<String> seen = new ArrayList<>();
    long started = System.nanoTime();
    try (StreamEvents events =
        client().streams().subscribeWithEvent(Map.of("event_type", "x", "event_data", Map.of()))) {
      events.forEachRemaining(event -> seen.add(StreamEvents.typeOf(event)));
    }
    long elapsedMillis = (System.nanoTime() - started) / 1_000_000;

    assertEquals(List.of("event:published", "run:fail"), seen);
    assertTrue(elapsedMillis >= 4_500 && elapsedMillis < 10_000, "after " + elapsedMillis);
  }

  @Test
  void subscribeWithEventFollowsAReplayedPendingRetryStop() {
    callStream(
        run("run:stop", "rA", "e1", "pending_retry", ",\"retry_attempt\":1"),
        run("run:stop", "rB", "e9", "succeeded",
            ",\"origin_run_id\":\"rA\",\"retry_attempt\":1,\"result\":\"recovered\""));

    List<String> seen = new ArrayList<>();
    try (StreamEvents events =
        client().streams().subscribeWithEvent(Map.of("event_type", "x", "event_data", Map.of()))) {
      events.forEachRemaining(
          event -> seen.add(StreamEvents.typeOf(event) + " " + StreamEvents.runIdOf(event)));
    }

    assertEquals(List.of("event:published null", "run:stop rA", "run:stop rB"), seen);
  }

  /** Stubs {@code responses} in order; the last one repeats. */
  private void sequence(
      String path, com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder... responses) {
    for (int i = 0; i < responses.length; i++) {
      com.github.tomakehurst.wiremock.client.ScenarioMappingBuilder mapping =
          get(urlPathEqualTo(path))
              .inScenario(path)
              .whenScenarioStateIs(
                  i == 0 ? com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED : "step" + i);
      if (i + 1 < responses.length) {
        mapping = mapping.willSetStateTo("step" + (i + 1));
      }
      server.stubFor(mapping.willReturn(responses[i]));
    }
  }

  // Task runs started by our run name it as origin_run_id with run_type "task" (and no
  // event_id); only a run of the origin's own run_type is a retry.

  private static final String TASK_STOP =
      run("run:stop", "t1", "null", "succeeded",
          ",\"run_type\":\"task\",\"origin_run_id\":\"r1\",\"result\":\"task result\"");

  @Test
  void taskRunNamingOurRunAsOriginDoesNotEndTheWait() {
    server.stubFor(get(urlPathEqualTo("/v1/streams/s1/subscribe"))
        .willReturn(ClientTest.sse(
            run("run:start", "r1", "e1", "running", ",\"run_type\":\"event\""),
            TASK_STOP,
            run("run:stop", "r1", "e1", "succeeded",
                ",\"run_type\":\"event\",\"result\":\"ours\""))));

    Map<String, Object> run = client().streams().waitForRunResult("s1", "e1");

    assertEquals("r1", run.get("run_id"));
    assertEquals("ours", run.get("result"));
  }

  @Test
  void taskRunBeforeOurRunTiesDoesNotMarkItRetried() {
    // The task run arrives first (an orphan keyed by r1); our plain failure still fails at once.
    server.stubFor(get(urlPathEqualTo("/v1/streams/s1/subscribe"))
        .willReturn(held(
            TASK_STOP,
            run("run:fail", "r1", "e1", "failed",
                ",\"run_type\":\"event\",\"retry_attempt\":0,\"result\":\"boom\""))));

    long started = System.nanoTime();
    HotRunException error =
        assertThrows(
            HotRunException.class, () -> client().streams().waitForRunResult("s1", "e1"));

    assertEquals("boom", error.getMessage());
    assertTrue((System.nanoTime() - started) / 1_000_000 < 3_000);
  }

  @Test
  void callHotIgnoresTaskRunsOfItsRun() {
    callStream(
        run("run:start", "r1", "e1", "running", ",\"run_type\":\"call\""),
        TASK_STOP,
        run("run:stop", "r1", "e1", "succeeded", ",\"run_type\":\"call\",\"result\":\"ours\""));

    assertEquals("ours", client().events().callHot("demo/fn", List.of()));
  }

  @Test
  void subscribeWithEventIgnoresTaskRunsOfItsRun() {
    callStream(
        TASK_STOP,
        run("run:start", "r1", "e1", "running", ",\"run_type\":\"event\""),
        run("run:stop", "r1", "e1", "succeeded", ",\"run_type\":\"event\""));

    List<String> seen = new ArrayList<>();
    try (StreamEvents events =
        client().streams().subscribeWithEvent(Map.of("event_type", "x", "event_data", Map.of()))) {
      events.forEachRemaining(
          event -> seen.add(StreamEvents.typeOf(event) + " " + StreamEvents.runIdOf(event)));
    }

    assertEquals(
        List.of("event:published null", "run:stop t1", "run:start r1", "run:stop r1"), seen);
  }

  @Test
  void exhaustedRetriesFail() {
    server.stubFor(get(urlPathEqualTo("/v1/streams/s1/subscribe"))
        .willReturn(ClientTest.sse(
            run("run:fail", "rA", "e1", "pending_retry",
                ",\"retry_attempt\":1,\"result\":\"first attempt\""),
            // The last retry: its attempt equals its origin's, so it fails without a grace.
            run("run:fail", "rB", "e9", "failed",
                ",\"origin_run_id\":\"rA\",\"retry_attempt\":1,\"result\":\"second attempt\""))));

    HotRunException error =
        assertThrows(
            HotRunException.class, () -> client().streams().waitForRunResult("s1", "e1"));

    assertEquals("second attempt", error.getMessage());
  }

  @Test
  void subscribeWithEventEndsAtTheRetryRun() {
    callStream(RETRY_SEQUENCE);

    List<String> seen = new ArrayList<>();
    try (StreamEvents events =
        client().streams().subscribeWithEvent(Map.of("event_type", "x", "event_data", Map.of()))) {
      events.forEachRemaining(
          event -> seen.add(StreamEvents.typeOf(event) + " " + StreamEvents.runIdOf(event)));
    }

    // The pending_retry failure is yielded but does not end the iterator; the retry's stop does.
    assertEquals(List.of("event:published null", "run:fail rA", "run:stop rB"), seen);
  }
}
