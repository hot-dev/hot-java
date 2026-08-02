package dev.hot.sdk;

import java.time.Duration;

/** Optional parameters for {@link RunsResource#waitFor}. */
public final class RunWaitOptions {

  private Duration timeout = Duration.ofSeconds(60);
  private int maxAttempts = 5;

  public RunWaitOptions timeout(Duration timeout) {
    this.timeout = timeout;
    return this;
  }

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
