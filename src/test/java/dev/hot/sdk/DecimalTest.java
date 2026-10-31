package dev.hot.sdk;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import com.github.tomakehurst.wiremock.WireMockServer;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** {@code useBigDecimalForFloats} keeps every digit of JSON decimals on every decode path. */
class DecimalTest {

  private static final String BIG = "12345678901234567890.123456789";
  private static final String STORED = "1234567890.123456789012";

  private WireMockServer server;

  @BeforeEach
  void start() {
    // HTTP/1.1 only, as the Hot API serves plain HTTP: the JDK 17 client
    // drops reused h2c connections mid-retry.
    server = new WireMockServer(options().dynamicPort().http2PlainDisabled(true));
    server.start();
    server.stubFor(
        post(urlPathEqualTo("/v1/streams/subscribe-with-event"))
            .willReturn(
                ClientTest.sse(
                    "{\"type\":\"event:published\",\"event_id\":\"e1\",\"stream_id\":\"s1\"}",
                    "{\"type\":\"run:stop\",\"run\":{\"run_id\":\"r1\",\"event_id\":\"e1\","
                        + "\"result\":{\"$ok\":" + BIG + "}}}")));
    server.stubFor(
        get(urlPathEqualTo("/v1/events/e1"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withBody(
                        "{\"data\":{\"event_id\":\"e1\",\"event_data\":{\"amount\":"
                            + STORED + ",\"count\":3}},\"meta\":{}}")));
  }

  @AfterEach
  void stop() {
    server.stop();
  }

  private HotClient client(boolean bigDecimals) {
    return HotClient.builder("test-token")
        .baseUrl(server.baseUrl())
        .useBigDecimalForFloats(bigDecimals)
        .build();
  }

  @Test
  void callHotResultKeepsEveryDigit() {
    Object result = client(true).events().callHot("demo/fn", List.of());

    assertEquals(new BigDecimal(BIG), result);
  }

  @Test
  void jsonResponsesKeepEveryDigit() {
    Map<String, Object> event = client(true).events().get("e1");
    Map<?, ?> data = (Map<?, ?>) event.get("event_data");

    assertEquals(new BigDecimal(STORED), data.get("amount"));
    assertEquals(3, data.get("count"));
  }

  @Test
  void streamEventsKeepEveryDigit() {
    try (StreamEvents events =
        client(true)
            .streams()
            .subscribeWithEvent(Map.of("event_type", "x", "event_data", Map.of()))) {
      Map<String, Object> last = null;
      while (events.hasNext()) {
        last = events.next();
      }
      Map<?, ?> result = (Map<?, ?>) StreamEvents.runOf(last).get("result");
      assertEquals(new BigDecimal(BIG), result.get("$ok"));
    }
  }

  @Test
  void defaultDecodesDoubles() {
    assertInstanceOf(Double.class, client(false).events().callHot("demo/fn", List.of()));
    Map<?, ?> data = (Map<?, ?>) client(false).events().get("e1").get("event_data");
    assertInstanceOf(Double.class, data.get("amount"));
  }
}
