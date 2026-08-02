package dev.hot.sdk;

import java.util.Map;

/** Thrown when a run fails or is cancelled; carries the durable run snapshot when available. */
public final class HotRunException extends HotStreamException {

  private final Map<String, Object> run;

  public HotRunException(String message) {
    this(message, null);
  }

  public HotRunException(String message, Map<String, Object> run) {
    super(message);
    this.run = run;
  }

  /** The terminal run snapshot, or {@code null} for legacy stream failures. */
  public Map<String, Object> run() {
    return run;
  }
}
