package dev.hot.sdk;

import java.util.Map;

/** Thrown when a waited task fails, is cancelled, or times out. */
public final class HotTaskException extends HotStreamException {

  private final Map<String, Object> task;

  public HotTaskException(String message, Map<String, Object> task) {
    super(message);
    this.task = task;
  }

  public Map<String, Object> task() {
    return task;
  }
}
