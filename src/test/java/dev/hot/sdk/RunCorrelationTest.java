package dev.hot.sdk;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

  @Test
  void replayedFinalStatusOfARetriedRunIsIgnored() {
    // After a reconnect the poll can replay the first attempt's failure with its final status.
    server.stubFor(get(urlPathEqualTo("/v1/streams/s1/subscribe"))
        .willReturn(ClientTest.sse(
            run("run:fail", "rA", "e1", "pending_retry", ",\"result\":\"first attempt\""),
            run("run:fail", "rA", "e1", "failed", ",\"result\":\"first attempt\""),
            run("run:stop", "rB", "e9", "succeeded",
                ",\"origin_run_id\":\"rA\",\"result\":\"recovered\""))));

    assertEquals("rB", client().streams().waitForRunResult("s1", "e1").get("run_id"));
  }

  @Test
  void exhaustedRetriesFail() {
    server.stubFor(get(urlPathEqualTo("/v1/streams/s1/subscribe"))
        .willReturn(ClientTest.sse(
            run("run:fail", "rA", "e1", "pending_retry", ",\"result\":\"first attempt\""),
            run("run:fail", "rB", "e9", "failed",
                ",\"origin_run_id\":\"rA\",\"result\":\"second attempt\""))));

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
