package dev.hot.sdk;

import java.time.Duration;
import java.util.function.Consumer;

/** Optional parameters for {@link StreamsResource#waitForRunResult}. */
public final class WaitOptions {

  private Duration timeout = Duration.ofSeconds(60);
  private Consumer<String> onChunk;

  /** Bounds the wait (default 60 seconds). */
  public WaitOptions timeout(Duration timeout) {
    this.timeout = timeout;
    return this;
  }

  /** Receives streamed text chunks from the matched run. */
  public WaitOptions onChunk(Consumer<String> onChunk) {
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
