package dev.hot.sdk;

/** Thrown when waiting for a run result exceeds the configured timeout. */
public final class HotTimeoutException extends HotStreamException {

  public HotTimeoutException(String message) {
    super(message);
  }
}
