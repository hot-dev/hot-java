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

  /**
   * Automatically resubscribe across the API's 5-minute SSE timeout (default true).
   */
  public SubscribeWithEventOptions reconnect(boolean reconnect) {
    this.reconnect = reconnect;
    return this;
  }

  /** Caps reconnection attempts (default 5). */
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
