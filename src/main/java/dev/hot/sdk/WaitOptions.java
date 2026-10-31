package dev.hot.sdk;

import java.time.Duration;
import java.util.function.Consumer;

/** Optional parameters for {@link StreamsResource#waitForRunResult}. */
public final class WaitOptions {

  private Duration timeout = Duration.ofSeconds(60);
  private Consumer<String> onChunk;
  private int maxAttempts = 5;

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

  /**
   * Caps consecutive failed reconnects (default 5). Connections that deliver events or end with
   * the API's 5-minute stream:complete rollover do not count, so long runs are bounded only by
   * the timeout.
   */
  public WaitOptions maxAttempts(int maxAttempts) {
    this.maxAttempts = maxAttempts;
    return this;
  }

  public Duration timeout() {
    return timeout;
  }

  public Consumer<String> onChunk() {
    return onChunk;
  }

  public int maxAttempts() {
    return maxAttempts;
  }
}
