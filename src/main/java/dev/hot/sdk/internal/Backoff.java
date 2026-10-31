package dev.hot.sdk.internal;

import dev.hot.sdk.HotApiException;
import dev.hot.sdk.HotTimeoutException;
import dev.hot.sdk.HotTransportException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * The reconnect policy shared by every durable waiter and subscribeWithEvent. Not part of the
 * public API.
 *
 * <ul>
 *   <li>Permanent errors (any 4xx except 408 and 429) stop immediately.
 *   <li>Transport errors, 408, 429 and 5xx reconnect after a jittered exponential backoff, and
 *       at least the server's Retry-After.
 *   <li>The budget counts consecutive failed connections. A connection that delivered new events
 *       or ended with stream:complete (the API's 5-minute rollover) resets it.
 *   <li>Backoff never sleeps past the caller's deadline.
 * </ul>
 */
public final class Backoff {

  static final long BASE_MILLIS = 500;
  static final long MAX_MILLIS = 10_000;
  private static final long MAX_RETRY_AFTER_MILLIS = 60_000;

  private Backoff() {}

  /** Jittered delay before consecutive failure {@code failures} (1-based): d/2 + rand(0, d/2). */
  public static long delayMillis(int failures) {
    int shift = Math.min(Math.max(failures, 1) - 1, 20);
    long cap = Math.min(MAX_MILLIS, BASE_MILLIS << shift);
    return cap / 2 + ThreadLocalRandom.current().nextLong(cap / 2 + 1);
  }

  /** The backoff for {@code error}, raised to the server's Retry-After when it sent one. */
  public static long reconnectDelayMillis(int failures, RuntimeException error) {
    long delay = delayMillis(failures);
    if (error instanceof HotApiException api && api.retryAfter() != null) {
      delay = Math.max(delay, Math.min(api.retryAfter() * 1000L, MAX_RETRY_AFTER_MILLIS));
    }
    return delay;
  }

  /** True when a reconnect cannot help: a permanent API error. */
  public static boolean isPermanent(RuntimeException error) {
    return error instanceof HotApiException api && api.isPermanent();
  }

  /**
   * Sleeps {@code delayMillis} before a reconnect. When the deadline ({@link System#nanoTime()}
   * based, or {@link Long#MAX_VALUE} for none) would pass first, throws {@link
   * HotTimeoutException} with {@code cause} instead.
   */
  public static void sleep(
      long delayMillis, long deadline, String timeoutMessage, RuntimeException cause) {
    if (deadline != Long.MAX_VALUE
        && deadline - System.nanoTime() < TimeUnit.MILLISECONDS.toNanos(delayMillis)) {
      throw new HotTimeoutException(timeoutMessage, cause);
    }
    try {
      Thread.sleep(delayMillis);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new HotTransportException(interrupted);
    }
  }
}
