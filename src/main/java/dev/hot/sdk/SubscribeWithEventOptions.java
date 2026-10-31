package dev.hot.sdk;

/** Optional parameters for {@link StreamsResource#subscribeWithEvent}. */
public final class SubscribeWithEventOptions {

  private String project;
  private boolean reconnect = true;
  private int maxAttempts = 5;

  /** Scopes the subscription to a project id or slug. */
  public SubscribeWithEventOptions project(String project) {
    this.project = project;
    return this;
  }

  /** Automatically resubscribe across the API's 5-minute SSE rollover (default true). */
  public SubscribeWithEventOptions reconnect(boolean reconnect) {
    this.reconnect = reconnect;
    return this;
  }

  /**
   * Caps consecutive failed reconnects (default 5). Connections that deliver events or end with
   * the API's 5-minute stream:complete rollover reset the count, so a long run never exhausts it.
   * Permanent errors (4xx other than 408 and 429) stop at once.
   */
  public SubscribeWithEventOptions maxAttempts(int maxAttempts) {
    this.maxAttempts = maxAttempts;
    return this;
  }

  public String project() {
    return project;
  }

  public boolean reconnectEnabled() {
    return reconnect;
  }

  public int maxAttempts() {
    return maxAttempts;
  }
}
