package dev.hot.sdk;

/** Thrown when the HTTP request itself fails (I/O error, interrupt, bad JSON). */
public final class HotTransportException extends RuntimeException {

  public HotTransportException(String message, Throwable cause) {
    super(message, cause);
  }

  public HotTransportException(Throwable cause) {
    super(cause.getMessage(), cause);
  }
}
