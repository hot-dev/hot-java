package dev.hot.sdk;

import java.time.Duration;

/** Optional parameters for {@link RunsResource#waitFor}. */
public final class RunWaitOptions {

  private Duration timeout = Duration.ofSeconds(60);
  private int maxAttempts = 5;

  /** Bounds the whole wait, reconnects and backoff included (default 60 seconds). */
  public RunWaitOptions timeout(Duration timeout) {
    this.timeout = timeout;
    return this;
  }

  /**
   * Caps consecutive failed reconnects (default 5). Connections that deliver a new snapshot or end
   * with the API's 5-minute stream:complete rollover do not count.
   */
  public RunWaitOptions maxAttempts(int maxAttempts) {
    this.maxAttempts = maxAttempts;
    return this;
  }

  public Duration timeout() {
    return timeout;
  }

  public int maxAttempts() {
    return maxAttempts;
  }
}
