package dev.hot.sdk;

import java.util.Iterator;
import java.util.Map;

/**
 * An iterator over Hot run-stream events in the Hot API wire format ({@code type},
 * {@code data_type}, {@code payload}, {@code run}, ...). Close it to abort the underlying
 * connection early; iterating to the end closes it automatically.
 */
public interface StreamEvents extends Iterator<Map<String, Object>>, AutoCloseable {

  @Override
  void close();

  /** The "type" field of an event map, or "" when absent. */
  static String typeOf(Map<String, Object> event) {
    return event.get("type") instanceof String type ? type : "";
  }

  /** The "run" object of an event map, or null when absent. */
  @SuppressWarnings("unchecked")
  static Map<String, Object> runOf(Map<String, Object> event) {
    return event.get("run") instanceof Map<?, ?> run ? (Map<String, Object>) run : null;
  }

  /** run.run_id of an event map, or null when absent. */
  static String runIdOf(Map<String, Object> event) {
    Map<String, Object> run = runOf(event);
    return run != null && run.get("run_id") instanceof String runId ? runId : null;
  }
}
