package dev.hot.sdk;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.exactly;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
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

class ReconnectTest {

  private static final String PUBLISHED =
      "{\"type\":\"event:published\",\"stream_id\":\"s1\",\"event_id\":\"e1\",\"event_type\":\"x\"}";

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

  private List<Map<String, Object>> collect(SubscribeWithEventOptions options) {
    List<Map<String, Object>> events = new ArrayList<>();
    try (StreamEvents stream =
        client()
            .streams()
            .subscribeWithEvent(Map.of("event_type", "x", "event_data", Map.of()), options)) {
      stream.forEachRemaining(events::add);
    }
    return events;
  }

  private static List<String> types(List<Map<String, Object>> events) {
    return events.stream().map(StreamEvents::typeOf).toList();
  }

  @Test
  void subscribeWithEventReconnectsViaStreamId() {
    server.stubFor(
        post(urlPathEqualTo("/v1/streams/subscribe-with-event"))
            .willReturn(
                ClientTest.sse(PUBLISHED, "{\"type\":\"run:start\",\"run\":{\"run_id\":\"r1\"}}")));
    server.stubFor(
        get(urlPathEqualTo("/v1/streams/s1/subscribe"))
            .willReturn(
                ClientTest.sse(
                    "{\"type\":\"run:start\",\"run\":{\"run_id\":\"r1\"}}",
                    "{\"type\":\"run:stop\",\"run\":{\"run_id\":\"r1\",\"result\":\"done\"}}")));

    List<Map<String, Object>> events = collect(new SubscribeWithEventOptions());

    assertEquals(List.of("event:published", "run:start", "run:stop"), types(events));
    server.verify(exactly(1), postRequestedFor(urlPathEqualTo("/v1/streams/subscribe-with-event")));
    server.verify(exactly(1), getRequestedFor(urlPathEqualTo("/v1/streams/s1/subscribe")));
  }

  @Test
  void subscribeWithEventNoReconnectReturnsAfterFirstConnection() {
    server.stubFor(
        post(urlPathEqualTo("/v1/streams/subscribe-with-event"))
            .willReturn(
                ClientTest.sse(PUBLISHED, "{\"type\":\"run:start\",\"run\":{\"run_id\":\"r1\"}}")));

    List<Map<String, Object>> events =
        collect(new SubscribeWithEventOptions().reconnect(false));

    assertEquals(List.of("event:published", "run:start"), types(events));
    assertEquals(1, server.getAllServeEvents().size());
  }

  @Test
  void subscribeWithEventDedupesRunEventsByRunId() {
    server.stubFor(
        post(urlPathEqualTo("/v1/streams/subscribe-with-event"))
            .willReturn(
                ClientTest.sse(PUBLISHED, "{\"type\":\"run:start\",\"run\":{\"run_id\":\"r1\"}}")));
    server.stubFor(
        get(urlPathEqualTo("/v1/streams/s1/subscribe"))
            .willReturn(
                ClientTest.sse(
                    "{\"type\":\"run:start\",\"run\":{\"run_id\":\"r1\"}}",
                    "{\"type\":\"run:start\",\"run\":{\"run_id\":\"r2\"}}",
                    "{\"type\":\"run:fail\",\"run\":{\"run_id\":\"r1\",\"result\":\"err\"}}",
                    "{\"type\":\"run:fail\",\"run\":{\"run_id\":\"r1\",\"result\":\"err\"}}")));

    List<Map<String, Object>> events = collect(new SubscribeWithEventOptions());

    List<String> startIds =
        events.stream()
            .filter(event -> StreamEvents.typeOf(event).equals("run:start"))
            .map(StreamEvents::runIdOf)
            .toList();
    List<String> failIds =
        events.stream()
            .filter(event -> StreamEvents.typeOf(event).equals("run:fail"))
            .map(StreamEvents::runIdOf)
            .toList();

    assertEquals(List.of("r1", "r2"), startIds);
    assertEquals(List.of("r1"), failIds);
  }

  @Test
  void subscribeWithEventThrowsWhenNoEventPublished() {
    server.stubFor(
        post(urlPathEqualTo("/v1/streams/subscribe-with-event"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "text/event-stream")
                    .withBody("")));

    HotStreamException error =
        assertThrows(
            HotStreamException.class, () -> collect(new SubscribeWithEventOptions()));

    assertTrue(error.getMessage().contains("event:published"), error.getMessage());
  }

  @Test
  void subscribePostUsesPostStreamEndpoint() {
    server.stubFor(
        post(urlPathEqualTo("/v1/streams/s1/subscribe"))
            .willReturn(ClientTest.sse("{\"type\":\"stream:complete\",\"stream_id\":\"s1\"}")));

    List<Map<String, Object>> events = new ArrayList<>();
    try (StreamEvents stream = client().streams().subscribePost("s1")) {
      stream.forEachRemaining(events::add);
    }

    assertEquals(List.of("stream:complete"), types(events));
    server.verify(exactly(1), postRequestedFor(urlPathEqualTo("/v1/streams/s1/subscribe")));
  }
}
