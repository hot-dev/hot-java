package dev.hot.sdk;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RunsTest {

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
    return HotClient.builder("token").baseUrl(server.baseUrl()).build();
  }

  @Test
  void waitForReconnectsAndReturnsSucceededSnapshot() {
    server.stubFor(
        get(urlPathEqualTo("/v1/runs/r1/subscribe"))
            .inScenario("run-wait")
            .whenScenarioStateIs(Scenario.STARTED)
            .willSetStateTo("reconnected")
            .willReturn(
                ClientTest.sse(
                    "{\"type\":\"run:update\",\"run\":{\"run_id\":\"r1\",\"status\":\"running\"}}")));
    server.stubFor(
        get(urlPathEqualTo("/v1/runs/r1/subscribe"))
            .inScenario("run-wait")
            .whenScenarioStateIs("reconnected")
            .willReturn(
                ClientTest.sse(
                    "{\"type\":\"run:update\",\"run\":{\"run_id\":\"r1\",\"status\":\"succeeded\",\"result\":\"done\"}}")));

    Map<String, Object> run = client().runs().waitFor("r1");
    assertEquals("succeeded", run.get("status"));
    assertEquals("done", run.get("result"));
    assertEquals(2, server.getAllServeEvents().size());
  }

  @Test
  void waitForThrowsStructuredFailure() {
    server.stubFor(
        get(urlPathEqualTo("/v1/runs/r1/subscribe"))
            .willReturn(
                ClientTest.sse(
                    "{\"type\":\"run:update\",\"run\":{\"run_id\":\"r1\",\"status\":\"failed\",\"result\":{\"error\":\"boom\"}}}")));

    HotRunException error =
        assertThrows(HotRunException.class, () -> client().runs().waitFor("r1"));
    assertEquals("boom", error.getMessage());
    assertEquals("failed", error.run().get("status"));
  }
}
