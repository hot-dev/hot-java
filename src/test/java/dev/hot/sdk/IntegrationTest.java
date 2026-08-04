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
  void waitForTaskStartedByEventHandler() {
    HotClient client = client();
    String taskId = null;
    String handlerRunId = null;
    String streamId = null;
    try (StreamEvents events =
        client
            .streams()
            .subscribeWithEvent(
                Map.of("event_type", "sdk:task", "event_data", Map.of("value", "hello")))) {
      while (events.hasNext()) {
        Map<String, Object> event = events.next();
        switch (StreamEvents.typeOf(event)) {
          case "event:published" -> streamId = (String) event.get("stream_id");
          case "run:fail", "run:cancel" -> fail("run did not complete: " + event);
          case "run:stop" -> {
            Map<String, Object> run = StreamEvents.runOf(event);
            handlerRunId = (String) run.get("run_id");
            Map<String, Object> result = castMap(run.get("result"));
            Map<String, Object> work = castMap(result.get("work"));
            taskId = (String) work.get("task");
          }
          default -> {}
        }
        if (taskId != null) {
          break;
        }
      }
    }

    assertNotNull(taskId, "run result did not contain work.task");
    assertNotNull(handlerRunId, "run:stop did not contain run_id");
    assertNotNull(streamId, "event:published did not contain stream_id");

    Map<String, Object> handlerRun = client.runs().waitFor(handlerRunId);
    assertEquals("succeeded", handlerRun.get("status"));

    Map<String, Object> streamedTask = null;
    try (StreamEvents events = client.streams().subscribe(streamId)) {
      while (events.hasNext()) {
        Map<String, Object> event = events.next();
        if (!"task:update".equals(StreamEvents.typeOf(event))) {
          continue;
        }
        Map<String, Object> task = castMap(event.get("task"));
        if (taskId.equals(task.get("task_id")) && "completed".equals(task.get("status"))) {
          streamedTask = task;
          break;
        }
      }
    }
    assertNotNull(streamedTask, "stream subscription did not receive completed task:update");

    Map<String, Object> task = client.tasks().waitFor(taskId);
    assertEquals("completed", task.get("status"));
    assertEquals(Map.of("processed", "hello"), task.get("result"));
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
