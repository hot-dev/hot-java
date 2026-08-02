package dev.hot.sdk;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.github.tomakehurst.wiremock.WireMockServer;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TasksTest {

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
    return HotClient.builder("token").baseUrl(server.baseUrl()).build();
  }

  @Test
  void waitForReturnsCompletedSnapshot() {
    server.stubFor(
        get(urlPathEqualTo("/v1/tasks/t1/subscribe"))
            .willReturn(
                ClientTest.sse(
                    "{\"type\":\"task:update\",\"task\":{\"task_id\":\"t1\",\"status\":\"completed\",\"result\":\"done\"}}")));

    Map<String, Object> task = client().tasks().waitFor("t1");
    assertEquals("completed", task.get("status"));
    assertEquals("done", task.get("result"));
  }

  @Test
  void waitForThrowsStructuredFailure() {
    server.stubFor(
        get(urlPathEqualTo("/v1/tasks/t1/subscribe"))
            .willReturn(
                ClientTest.sse(
                    "{\"type\":\"task:update\",\"task\":{\"task_id\":\"t1\",\"status\":\"failed\",\"result\":{\"error\":\"boom\"}}}")));

    HotTaskException error =
        assertThrows(HotTaskException.class, () -> client().tasks().waitFor("t1"));
    assertEquals("boom", error.getMessage());
    assertEquals("failed", error.task().get("status"));
  }
}
