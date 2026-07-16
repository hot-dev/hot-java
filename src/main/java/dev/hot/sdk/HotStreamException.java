package dev.hot.sdk;

/** Thrown when a run stream ends before the expected event arrives. */
public class HotStreamException extends RuntimeException {

  public HotStreamException(String message) {
    super(message);
  }
}
