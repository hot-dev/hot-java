package dev.hot.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Integration tests against a live `hot dev` instance serving the fixture project in
 * integration/fixture. Start one and run this class with integration/run.sh; without
 * HOT_TEST_API_KEY set, the whole class is skipped.
 */
@EnabledIfEnvironmentVariable(named = "HOT_TEST_API_KEY", matches = ".+")
class IntegrationTest {

  private static HotClient client() {
    String baseUrl = System.getenv("HOT_TEST_BASE_URL");
    return HotClient.builder(System.getenv("HOT_TEST_API_KEY"))
        .baseUrl(baseUrl != null ? baseUrl : "http://localhost:4681")
        .build();
  }

  @Test
  void envAndProjects() {
    HotClient client = client();

    Map<String, Object> env = client.env().get();
    assertEquals("development", env.get("name"));

    Map<String, Object> projects = client.projects().list();
    boolean found =
        ((List<?>) projects.get("data"))
            .stream()
                .anyMatch(
                    project ->
                        project instanceof Map<?, ?> p && "sdk-fixture".equals(p.get("name")));
    assertTrue(found, "expected a project named sdk-fixture: " + projects.get("data"));
  }

  @Test
  void subscribeWithEventEcho() {
    Map<String, Object> result = null;
    try (StreamEvents events =
        client()
            .streams()
            .subscribeWithEvent(
                Map.of("event_type", "sdk:echo", "event_data", Map.of("text", "hello")))) {
      while (events.hasNext()) {
        Map<String, Object> event = events.next();
        switch (StreamEvents.typeOf(event)) {
          case "run:fail", "run:cancel" -> fail("run did not complete: " + event);
          case "run:stop" -> {
            Object raw = StreamEvents.runOf(event).get("result");
            result = raw instanceof Map<?, ?> ? castMap(raw) : null;
          }
          default -> {}
        }
        if (result != null) {
          break;
        }
      }
    }

    assertNotNull(result, "no run:stop observed");
    assertEquals(Map.of("text", "hello"), result.get("echoed"));
  }

  @Test
  void callHotAddNums() {
    Object result = client().events().callHot("::fixture::sdk/add-nums", List.of(2, 3));
    assertEquals(5, result);
  }

  @Test
  void runFailureMessage() {
    HotClient client = client();
    Map<String, Object> published =
        client.events().publish(Map.of("event_type", "sdk:fail", "event_data", Map.of()));

    HotRunException error =
        assertThrows(
            HotRunException.class,
            () ->
                client
                    .streams()
                    .waitForRunResult(
                        (String) published.get("stream_id"),
                        (String) published.get("event_id")));
    assertTrue(
        error.getMessage().contains("sdk fixture failure"),
        "unexpected failure message: " + error.getMessage());
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> castMap(Object value) {
    return (Map<String, Object>) value;
  }
}
