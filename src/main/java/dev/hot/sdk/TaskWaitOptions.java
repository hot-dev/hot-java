package dev.hot.sdk;

import java.time.Duration;

/** Optional parameters for {@link TasksResource#waitFor}. */
public final class TaskWaitOptions {

  private Duration timeout = Duration.ofSeconds(60);
  private int maxAttempts = 5;

  public TaskWaitOptions timeout(Duration timeout) {
    this.timeout = timeout;
    return this;
  }

  public TaskWaitOptions maxAttempts(int maxAttempts) {
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
