package dev.hot.sdk;

/** Thrown when a run ends with run:fail or run:cancel; carries the run's result message. */
public final class HotRunException extends HotStreamException {

  public HotRunException(String message) {
    super(message);
  }
}
