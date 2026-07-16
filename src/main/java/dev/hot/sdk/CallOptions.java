package dev.hot.sdk;

import java.time.Duration;
import java.util.function.Consumer;

/** Optional parameters for {@link EventsResource#callHot}. */
public final class CallOptions {

  private Duration timeout = Duration.ofSeconds(60);
  private Consumer<String> onChunk;

  /** Bounds the wait for the run result (default 60 seconds). */
  public CallOptions timeout(Duration timeout) {
    this.timeout = timeout;
    return this;
  }

  /** Receives streamed text chunks from the run. */
  public CallOptions onChunk(Consumer<String> onChunk) {
    this.onChunk = onChunk;
    return this;
  }

  public Duration timeout() {
    return timeout;
  }

  public Consumer<String> onChunk() {
    return onChunk;
  }
}
